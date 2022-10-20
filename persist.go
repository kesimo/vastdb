package vastdb

import (
	"bufio"
	"errors"
	"github.com/kesimo/vastdb/internal/tree"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNoFile is returned when the file does not exist.
	ErrNoFile = errors.New("file does not exist")
	// ErrLoad is returned when the file cannot be loaded.
	ErrLoad = errors.New("file cannot be loaded")
	// ErrSyncFile is returned when the file cannot be synced.
	ErrSyncFile = errors.New("file cannot be synced")
)

type persistence[T any] struct {
	DB                   *DB[T]
	isActive             bool
	path                 string
	file                 *os.File
	buf                  []byte
	flushes              int
	isShrinking          bool
	lastShrinkSize       int
	AutoShrinkMinSize    int
	AutoShrinkPercentage int
}

// Open called once a database is created/opened.
// It will open the file and load the data into the database if possible.
// If the file does not exist, it will be created.
func (p *persistence[T]) Open() error {
	// do nothing if persisting is disabled
	if !p.isActive {
		return nil
	}
	var err error
	// hard coding 0666 as the default mode.
	p.file, err = os.OpenFile(p.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// load the database from disk
	if err := p.load(); err != nil {
		// close on error, ignore close error
		_ = p.file.Close()
		return err
	}
	return nil
}

// Close called once a database is closed.
// It will sync the file and close it.
// in case of sync error, the file will be closed anyway.
func (p *persistence[T]) Close() error {
	// do nothing if persisting is disabled
	if !p.isActive {
		return nil
	}
	var errOut error
	if err := p.file.Sync(); err != nil {
		errOut = ErrSyncFile
	}
	if err := p.file.Close(); err != nil {
		return err
	}
	return errOut
}

// FileSync sync the underlying file.
func (p *persistence[T]) FileSync() error {
	// do nothing if persisting is disabled
	if !p.isActive {
		return nil
	}
	return p.file.Sync()
}

// AutoShrink will shrink the file if it's size is more than 2x the size of the database.
func (p *persistence[T]) AutoShrink() (shrink bool, err error) {
	// do nothing if persisting is disabled
	if !p.isActive {
		return shrink, nil
	}
	// do nothing if shrinking is disabled
	if !p.isShrinking {
		return shrink, nil
	}
	pos, err := p.file.Seek(0, 1)
	if err != nil {
		return shrink, err
	}
	aofsz := int(pos)
	if aofsz > p.AutoShrinkMinSize {
		prc := float64(p.AutoShrinkPercentage) / 100.0
		shrink = aofsz > p.lastShrinkSize+int(float64(p.lastShrinkSize)*prc)
	}
	return shrink, nil
}

// Shrink will shrink the file to the size of the database.
// It does not block the database.
func (p *persistence[T]) Shrink() error {
	p.DB.mu.Lock()
	if p.DB.closed {
		p.DB.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !p.isActive {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		p.DB.mu.Unlock()
		return nil
	}
	if p.isShrinking {
		// The database is already in the process of shrinking.
		p.DB.mu.Unlock()
		return ErrShrinkInProcess
	}
	p.isShrinking = true
	defer func() {
		p.DB.mu.Lock()
		p.isShrinking = false
		p.DB.mu.Unlock()
	}()
	filename := p.file.Name()
	tmp := filename + ".tmp"
	// go back to end after writing with endPosition
	endPosition, err := p.file.Seek(0, 2)
	if err != nil {
		return err
	}
	p.DB.mu.Unlock()
	// TODO: Why ??
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmp)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			p.DB.mu.RLock()
			defer p.DB.mu.RUnlock()
			if p.DB.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			now := time.Now()
			pivItem := &dbItem[T]{key: pivot}
			p.DB.keys.AscendGTE(&pivItem,
				func(item *dbItem[T]) bool {
					dbi := item
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf, now, p.DB.isBiType)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a deferred
		// lock/unlock.
		p.DB.mu.Lock()
		defer p.DB.mu.Unlock()
		if p.DB.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endPosition, 0); err != nil {
			return err
		}
		// Just copy all the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := p.file.Close(); err != nil {
			return err
		}
		// Any failures below here are awful. So just panic.
		if err := os.Rename(tmp, filename); err != nil {
			_ = panicErr(err)
		}
		p.file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			_ = panicErr(err)
		}
		pos, err := p.file.Seek(0, 2)
		if err != nil {
			return err
		}
		p.lastShrinkSize = int(pos)
		return nil
	}()
}

// load will load the database items from the append-only file.
// The file format uses the Redis append only file format RESP commands.
// For more information on RESP please read http://redis.io/topics/protocol.
// Supported commands are DEL and SET.
func (p *persistence[T]) load() error {
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	n, err := p.LoadFromReader(p.file, fi.ModTime())
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// The db file has ended mid-command, which is allowed but the
			// data file should be truncated to the end of the last valid
			// command
			if err := p.file.Truncate(n); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := p.file.Seek(n, 0); err != nil {
		return err
	}
	var estaofsz int
	p.DB.keys.Walk(func(items []*dbItem[T]) {
		for _, v := range items {
			estaofsz += v.estAOFSetSize(p.DB.isBiType)
		}
	})
	p.lastShrinkSize += estaofsz
	return nil
}

// LoadFromReader will load database commands from the io.Reader.
// It will return the number of bytes read and any error encountered.
// The time is the time the reader was last modified. (used for TTL, not greater than now)
func (p *persistence[T]) LoadFromReader(rd io.Reader, modTime time.Time) (n int64, err error) {
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()
	totalSize := int64(0)
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// peek at the first byte. If it's a 'nul' control character then
		// ignore it and move to the next byte.
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return totalSize, err
		}
		if c == 0 {
			// ignore nul control characters
			n += 1
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return totalSize, err
		}

		// read a single command.
		// first we should read the number of parts that the of the command
		cmdByteSize := int64(0)
		line, err := r.ReadBytes('\n')
		if err != nil {
			return totalSize, err
		}
		if line[0] != '*' {
			return totalSize, ErrInvalid
		}
		cmdByteSize += int64(len(line))

		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return totalSize, ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return totalSize, ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return totalSize, ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return totalSize, err
			}
			if line[0] != '$' {
				return totalSize, ErrInvalid
			}
			cmdByteSize += int64(len(line))
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return totalSize, ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return totalSize, ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return totalSize, ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return totalSize, err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return totalSize, ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
			cmdByteSize += int64(n + 2)
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return totalSize, ErrInvalid
			}
			if len(parts) == 5 {
				if strings.ToLower(parts[3]) != "ex" {
					return totalSize, ErrInvalid
				}
				ex, err := strconv.ParseUint(parts[4], 10, 64)
				if err != nil {
					return totalSize, err
				}
				now := time.Now()
				dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
				if dur > 0 {
					var valT T
					if err := valueFromString(parts[2], &valT); err != nil {
						return totalSize, err
					}
					p.DB.insertIntoDatabase(&dbItem[T]{
						key: parts[1],
						val: valT,
						opts: &dbItemOpts{
							ex:   true,
							exat: now.Add(dur),
						},
					})
				}
			} else {
				var valT T
				if err := valueFromString(parts[2], &valT); err != nil {
					return totalSize, err
				}
				p.DB.insertIntoDatabase(&dbItem[T]{key: parts[1], val: valT})
			}
		} else if (parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			p.DB.deleteFromDatabase(&dbItem[T]{key: parts[1]})
		} else if (parts[0][0] == 'f' || parts[0][0] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			p.DB.keys = tree.NewGBtree[*dbItem[T]](lessCtx[T](nil))
			p.DB.exps = tree.NewGBtree[*dbItem[T]](lessCtx[T](&exctx[T]{p.DB}))
			p.DB.idxs = make(map[string]*index[T])
		} else {
			return totalSize, ErrInvalid
		}
		totalSize += cmdByteSize
	}
}

func (p *persistence[T]) txCommit(tx *Tx[T], err error) error {
	if p.isActive && (len(tx.wc.commitItems) > 0 || tx.wc.rbKeys != nil) {
		p.buf = p.buf[:0]
		// write a flushdb if a deleteAll was called.
		if tx.wc.rbKeys != nil {
			p.buf = append(p.buf, "*1\r\n$7\r\nflushdb\r\n"...)
		}
		now := time.Now()
		// Each committed record is written to disk
		for key, item := range tx.wc.commitItems {
			if item == nil {
				p.buf = (&dbItem[T]{key: key}).writeDeleteTo(p.buf)
			} else {
				p.buf = item.writeSetTo(p.buf, now, tx.db.isBiType)
			}
		}
		// Flushing the buffer only once per transaction.
		// If this operation fails then also write failed, and we must
		// roll back.
		var n int
		n, err = p.file.Write(p.buf)
		if err != nil {
			if n > 0 {
				// There was a partial write to disk.
				// We are possibly out of disk space.
				// Delete the partially written bytes from the data file by
				// seeking to the previously known position and performing
				// a truncate operation.
				// At this point a syscall failure is fatal and the process
				// should be killed to avoid corrupting the file.
				pos, err := p.file.Seek(-int64(n), 1)
				if err != nil {
					_ = panicErr(err)
				}
				if err := p.file.Truncate(pos); err != nil {
					_ = panicErr(err)
				}
			}
			tx.rollbackInner()
		}
		if tx.db.config.SyncPolicy == Always {
			_ = p.file.Sync()
		}
		// Increment the number of flushes. The background syncing uses this.
		p.flushes++
	}
	return err
}

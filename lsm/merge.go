package lsm

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"time"

	"github.com/wubba-com/lsm-distributed/lsm/sst"
)

// MergeJob runs as a background thread and coordinates when to check SST levels for merging.
func (t *LSMTree) mergeJob() {
	defer t.wg.Done()
	if t.config.Merge.Interval == 0 {
		log.Println("mergeJob interval not set, stopping goroutine")
		return
	}
	ticker := time.NewTicker(t.config.Merge.Interval)

	for {
		select {
		case <-ticker.C:
			//log.Println("LSM merge job woke up")
			if err := t.merge(); err != nil {
				t.logger.Debug(err.Error())
			}
		case <-t.ctx.Done():
			return
		}

	}
}

func (s *LSMTree) SetMergeSettings(ms MergeSettings) {
	s.config.Merge = ms
}

func (t *LSMTree) merge() error {
	lvls, err := sst.Levels(t.root)
	if err != nil {
		return err
	}

	//t.logger.Debug("мем уровни", slog.Any("lvls", lvls))

	for _, lvl := range lvls {
		files, err := sst.Filename(t.root, lvl)
		if err != nil {
			return err
		}

		var (
			isMerge = false
			num     = t.config.Merge.NumberOfSstFiles
		)
		if t.debug {
			t.logger.Debug("нужно сливать?", slog.Bool("is merge", num > 0 && len(files) > num*int(lvl+1)), slog.Int("lvl", int(lvl)))
		}
		if num > 0 && len(files) > num*int(lvl+1) {
			log.Printf("merge level %d, number of files %d exceeded merge threshold", lvl, len(files))
			isMerge = true

			if lvl == sst.Level(t.config.Merge.MaxLevels) {
				// условия для последнего уровня
				isMerge = false
			}
		}

		if isMerge {
			t.compact(lvl)
		}
	}

	return nil
}

// Merge берет все текущие SST-файлы на уровне и объединяет их с
// SST-файлами на следующем уровне дерева LSM. Во время этого
// процесса данные уплотняются, и все старые значения ключей или надгробные плиты удаляются безвозвратно.
func (t *LSMTree) compact(level sst.Level) error {
	// Общий алгоритм
	//
	// - найти путь к уровню, получить все sst-файлы
	// - найти путь для уровня+1, получить все sst-файлы
	// - загружаем содержимое файлов в кучу (в будущем: передаем их потоком)
	// - выписать файлы обратно в новый временный каталог
	// - получить блокировку дерева
	// - поменять местами уровень+1 с новым каталогом
	// - удалить все старые файлы
	// - очистить все данные в памяти для файлов
	// - снять блокировки, слияние завершено
	// - записываем в syslog, считаем WAL
	// TODO: если level == tree.merge.MaxLevels, то уплотнить этот уровень вместо слияния в l+1

	currentMaxLvl := sst.Level(len(t.levels))
	if level > currentMaxLvl {
		desc := fmt.Sprintf("merge cannot process level %d because the tree only has %d levels", level, currentMaxLvl)
		log.Println(desc)

		return errors.New(desc)
	}

	if level > 0 && level == sst.Level(t.config.Merge.MaxLevels) {
		// if max lvl

		return nil
	}

	currentLvlFiles, err := sst.Filename(t.root, level)
	if err != nil {
		return err
	}

	nextLvlFiles, err := sst.Filename(t.root, level+1)
	if err != nil {
		return err
	}

	currentLvlFiles = append(currentLvlFiles, nextLvlFiles...)

	if t.debug {
		t.logger.Debug("файлы для уплотнения", slog.Any("files", currentLvlFiles))
	}

	var removedTombstone bool
	if level == currentMaxLvl {
		removedTombstone = true
	}

	meta, err := sst.Compact(t.root, currentLvlFiles, level, t.config.MemtblDataSize*uint32(math.Pow(2, float64(level+1))), t.sparseKeyDistance, removedTombstone)
	if err != nil {
		return err
	}

	if !t.config.Merge.Immediate {
		t.lock.Lock()
		defer t.lock.Unlock()
	}

	for idx := range currentLvlFiles {
		if err := sst.Remove(t.root, currentLvlFiles[idx].Level, currentLvlFiles[idx].SeqNum); err != nil {
			return err
		}
	}

	if meta.Level-level == 1 {
		t.levels = append(t.levels, sst.SSTLevel{})
		t.levels[meta.Level].Files = append(t.levels[meta.Level].Files, meta)
		t.levels[level] = sst.SSTLevel{}
	} else {
		return fmt.Errorf("the level is too high")
	}

	if t.debug {
		t.logger.Debug("уплотнение закончено", slog.Int("lvls", int(level)), slog.Any("lvls", t.levels))
	}

	return nil
}

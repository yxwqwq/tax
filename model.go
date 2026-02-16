package tax

import (
	"os"
	"github.com/jinzhu/gorm"
	_ "github.com/mattn/go-sqlite3"
)

type TaxDB struct {
	db *gorm.DB
}

type TaxRecord struct {
	gorm.Model
	UserID    int64  `gorm:"column:user_id;index"`
	GroupID   int64  `gorm:"column:group_id;index"`
	TaxAmount int    `gorm:"column:tax_amount"`
	TaxTime   int64  `gorm:"column:tax_time;index"`
	UserName  string `gorm:"column:user_name"`
}

type UserTaxRate struct {
	gorm.Model
	UserID  int64   `gorm:"column:user_id;unique_index:idx_user_group"`
	GroupID int64   `gorm:"column:group_id;unique_index:idx_user_group"`
	Rate    float64 `gorm:"column:rate"`
}

type TreasuryLog struct {
	gorm.Model
	Amount      int    `gorm:"column:amount"`
	Operation   string `gorm:"column:operation"`
	Operator    int64  `gorm:"column:operator"`
	OpTime      int64  `gorm:"column:op_time;index"`
	Description string `gorm:"column:description"`
}

func InitDatabase(dbPath string) (*TaxDB, error) {
	var err error
	if _, err = os.Stat(dbPath); err != nil || os.IsNotExist(err) {
		f, err := os.Create(dbPath)
		if err != nil {
			return nil, err
		}
		defer f.Close()
	}
	
	db, err := gorm.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	
	db.AutoMigrate(&TaxRecord{}, &UserTaxRate{}, &TreasuryLog{})
	
	return &TaxDB{db: db}, nil
}

func (tdb *TaxDB) InsertTaxRecord(record TaxRecord) error {
	return tdb.db.Create(&record).Error
}

func (tdb *TaxDB) GetTaxRecordsByUserID(userID int64, limit int) ([]TaxRecord, error) {
	var records []TaxRecord
	err := tdb.db.Where("user_id = ?", userID).Order("created_at DESC").Limit(limit).Find(&records).Error
	return records, err
}

func (tdb *TaxDB) GetUserTaxRate(userID, groupID int64) (float64, error) {
	var rate UserTaxRate
	err := tdb.db.Where("user_id = ? AND group_id = ?", userID, groupID).First(&rate).Error
	return rate.Rate, err
}

func (tdb *TaxDB) SetUserTaxRate(userID, groupID int64, rate float64) error {
	var ur UserTaxRate
	err := tdb.db.Where("user_id = ? AND group_id = ?", userID, groupID).First(&ur).Error
	
	if err != nil && !tdb.db.RecordNotFound() {
		return err
	}
	
	if tdb.db.RecordNotFound() {
		return tdb.db.Create(&UserTaxRate{
			UserID:  userID,
			GroupID: groupID,
			Rate:    rate,
		}).Error
	} else {
		return tdb.db.Model(&ur).Update("rate", rate).Error
	}
}

func (tdb *TaxDB) InsertTreasuryLog(log TreasuryLog) error {
	return tdb.db.Create(&log).Error
}

func (tdb *TaxDB) GetTreasuryTotal() (int, error) {
	var total int
	err := tdb.db.Table("treasury_logs").Select("COALESCE(SUM(CASE WHEN operation IN ('INCOME', 'TAX_INCOME') THEN amount ELSE -amount END), 0)").Row().Scan(&total)
	return total, err
}

func (tdb *TaxDB) GetTaxRankings(groupID int64, limit int) ([]TaxRecord, error) {
	var records []TaxRecord
	err := tdb.db.Table("tax_records").
		Select("user_id, user_name, sum(tax_amount) as tax_amount").
		Where("group_id = ?", groupID).
		Group("user_id").
		Order("sum(tax_amount) DESC").
		Limit(limit).
		Find(&records).Error
	return records, err
}
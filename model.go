package tax

import (
	"github.com/FloatTech/floatbox/file"
	sql "github.com/FloatTech/sqlite"
)

type TaxDB struct {
	db sql.Sqlite
}

type TaxRecord struct {
	ID        uint   `gorm:"primarykey;autoIncrement"`
	UserID    int64  `gorm:"column:user_id;index:idx_user_time"`
	GroupID   int64  `gorm:"column:group_id;index:idx_user_time"`
	TaxAmount int    `gorm:"column:tax_amount"`
	TaxTime   int64  `gorm:"column:tax_time;index:idx_user_time"`
	UserName  string `gorm:"column:user_name"`
}

type UserTaxRate struct {
	ID      uint    `gorm:"primarykey;autoIncrement"`
	UserID  int64   `gorm:"column:user_id;index:idx_user_group,unique"`
	GroupID int64   `gorm:"column:group_id;index:idx_user_group,unique"`
	Rate    float64 `gorm:"column:rate"`
}

type TreasuryLog struct {
	ID          uint   `gorm:"primarykey;autoIncrement"`
	Amount      int    `gorm:"column:amount"`
	Operation   string `gorm:"column:operation"`
	Operator    int64  `gorm:"column:operator"`
	OpTime      int64  `gorm:"column:op_time;index:idx_op_time"`
	Description string `gorm:"column:description"`
}

func (db *TaxDB) Open(dbfile string) error {
	db.db.File = dbfile
	return db.db.Open()
}

func (db *TaxDB) Close() error {
	return db.db.Close()
}

func (db *TaxDB) IsExists() bool {
	return file.IsExist(db.db.File)
}

func (db *TaxDB) CreateTables() error {
	err := db.db.CreateAndFirstOrCreateTable("tax_records", func(u *sql.Sqlite) error {
		return u.CreateIndex("tax_records", "idx_user_time", "user_id,group_id,tax_time")
	})
	if err != nil {
		return err
	}
	
	err = db.db.CreateAndFirstOrCreateTable("user_tax_rates", func(u *sql.Sqlite) error {
		return u.CreateIndex("user_tax_rates", "idx_user_group", "user_id,group_id")
	})
	if err != nil {
		return err
	}
	
	return db.db.CreateAndFirstOrCreateTable("treasury_logs", func(u *sql.Sqlite) error {
		return u.CreateIndex("treasury_logs", "idx_op_time", "op_time")
	})
}

func (db *TaxDB) InsertTaxRecord(record TaxRecord) error {
	return db.db.Insert("tax_records", &record)
}

func (db *TaxDB) GetTaxRecordsByUserID(userID int64, limit int) ([]TaxRecord, error) {
	var records []TaxRecord
	err := db.db.FindFor("tax_records", &records, "WHERE user_id = ? ORDER BY tax_time DESC LIMIT ?", userID, limit)
	return records, err
}

func (db *TaxDB) GetUserTaxRate(userID, groupID int64) (float64, error) {
	var rate UserTaxRate
	err := db.db.Find("user_tax_rates", &rate, "WHERE user_id = ? AND group_id = ?", userID, groupID)
	return rate.Rate, err
}

func (db *TaxDB) SetUserTaxRate(userID, groupID int64, rate float64) error {
	return db.db.Insert("user_tax_rates", &UserTaxRate{
		UserID: userID,
		GroupID: groupID,
		Rate:   rate,
	})
}

func (db *TaxDB) InsertTreasuryLog(log TreasuryLog) error {
	return db.db.Insert("treasury_logs", &log)
}

func (db *TaxDB) GetTreasuryTotal() (int, error) {
	var total int
	err := db.db.Count("treasury_logs", &total, "SELECT IFNULL(SUM(CASE WHEN operation IN ('INCOME', 'TAX_INCOME') THEN amount ELSE -amount END), 0) FROM treasury_logs")
	return total, err
}

func (db *TaxDB) GetTaxRankings(groupID int64, limit int) ([]TaxRecord, error) {
	var records []TaxRecord
	err := db.db.FindFor("tax_records", &records, 
		"WHERE group_id = ? GROUP BY user_id ORDER BY SUM(tax_amount) DESC LIMIT ?", 
		groupID, limit)
	return records, err
}
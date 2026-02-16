package tax

import (
	"github.com/FloatTech/floatbox/file"
	sql "github.com/FloatTech/sqlite"
)

type TaxDB struct {
	db sql.Sqlite
}

type TaxRecord struct {
	ID        uint   `db:"id"`
	UserID    int64  `db:"user_id"`
	GroupID   int64  `db:"group_id"`
	TaxAmount int    `db:"tax_amount"`
	TaxTime   int64  `db:"tax_time"`
	UserName  string `db:"user_name"`
}

type UserTaxRate struct {
	ID      uint    `db:"id"`
	UserID  int64   `db:"user_id"`
	GroupID int64   `db:"group_id"`
	Rate    float64 `db:"rate"`
}

type TreasuryLog struct {
	ID          uint   `db:"id"`
	Amount      int    `db:"amount"`
	Operation   string `db:"operation"`
	Operator    int64  `db:"operator"`
	OpTime      int64  `db:"op_time"`
	Description string `db:"description"`
}

func (db *TaxDB) Open(dbfile string) error {
	db.db.File = dbfile
	return db.db.Open(10)
}

func (db *TaxDB) Close() error {
	return db.db.Close()
}

func (db *TaxDB) IsExists() bool {
	return file.IsExist(db.db.File)
}

func (db *TaxDB) CreateTables() error {
	err := db.db.Create("tax_records", &TaxRecord{})
	if err != nil {
		return err
	}
	
	err = db.db.Create("user_tax_rates", &UserTaxRate{})
	if err != nil {
		return err
	}
	
	return db.db.Create("treasury_logs", &TreasuryLog{})
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
	if err != nil {
		return 0, err
	}
	return rate.Rate, nil
}

func (db *TaxDB) SetUserTaxRate(userID, groupID int64, rate float64) error {
	// 先尝试查找是否存在记录
	var existing UserTaxRate
	err := db.db.Find("user_tax_rates", &existing, "WHERE user_id = ? AND group_id = ?", userID, groupID)
	if err != nil {
		// 如果记录不存在，则插入新记录
		return db.db.Insert("user_tax_rates", &UserTaxRate{
			UserID:  userID,
			GroupID: groupID,
			Rate:    rate,
		})
	} else {
		// 如果记录存在，则更新记录
		return db.db.Update("user_tax_rates", &UserTaxRate{
			ID:      existing.ID,
			UserID:  userID,
			GroupID: groupID,
			Rate:    rate,
		}, "WHERE user_id = ? AND group_id = ?", userID, groupID)
	}
}

func (db *TaxDB) InsertTreasuryLog(log TreasuryLog) error {
	return db.db.Insert("treasury_logs", &log)
}

func (db *TaxDB) GetTreasuryTotal() (int, error) {
	var total int
	err := db.db.Count("treasury_logs", &total, "SELECT COALESCE(SUM(CASE WHEN operation IN ('INCOME', 'TAX_INCOME') THEN amount ELSE -amount END), 0) FROM treasury_logs")
	return total, err
}

func (db *TaxDB) GetTaxRankings(groupID int64, limit int) ([]TaxRecord, error) {
	var records []TaxRecord
	err := db.db.FindFor("tax_records", &records, 
		"WHERE group_id = ? GROUP BY user_id ORDER BY SUM(tax_amount) DESC LIMIT ?", 
		groupID, limit)
	return records, err
}
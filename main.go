// Package tax 税收系统
package tax

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/FloatTech/AnimeAPI/wallet"
	"github.com/FloatTech/floatbox/binary"
	"github.com/FloatTech/floatbox/file"
	"github.com/FloatTech/floatbox/process"
	ctrl "github.com/FloatTech/zbpctrl"
	"github.com/FloatTech/zbputils/control"
	zero "github.com/wdvxdr1123/ZeroBot"
	"github.com/wdvxdr1123/ZeroBot/message"
	"github.com/sirupsen/logrus"
)

const (
	defaultTaxRate      = 0.1                  // 默认税率10%
	defaultThreshold    = 1000                 // 起征点1000
	autoTaxHour         = 12                   // 自动征税时间（12点）
)

var (
	taxConfig = struct {
		sync.RWMutex
		TaxRate    float64 // 税率
		Threshold  int64   // 起征点
		LastTaxDay string  // 上次征税日期
	}{
		TaxRate:    defaultTaxRate,
		Threshold:  defaultThreshold,
		LastTaxDay: "",
	}
	taxDB TaxDB
)

func init() {
	engine := control.AutoRegister(&ctrl.Options[*zero.Ctx]{
		DisableOnDefault: false,
		Brief:            "税收系统",
		Help: "税收系统功能:\n" +
			"- 设置税率 [0-1之间的数字，如0.1表示10%]\n" +
			"- 设置起征点 [金额]\n" +
			"- 查看税收设置\n" +
			"- 个人税率 [@某人] [税率，可选]\n" +
			"- 查看我的缴税记录\n" +
			"- 查看税收排行榜\n" +
			"- 手动征收个人税 [@某人]\n" +
			"- 手动征收全服税\n" +
			"- 查看国库\n" +
			"提示: 税收每天中午12点自动征收一次，超过起征点的部分按税率收取",
		PrivateDataFolder: "tax",
	})

	dbFile := engine.DataFolder() + "tax.db"
	
	go func() {
		// 初始化数据库
		err := taxDB.Open(dbFile)
		if err != nil {
			panic(err)
		}
		
		// 创建数据表
		err = taxDB.CreateTables()
		if err != nil {
			panic(err)
		}
		
		// 尝试加载配置
		loadConfig(engine.DataFolder() + "config.txt")
		
		// 启动自动税收定时器
		go autoTaxRoutine()
	}()

	// 设置税率
	engine.OnRegex(`^设置税率\s*([0-9]+\.?[0-9]*)$`, zero.SuperUserPermission).SetBlock(true).Handle(func(ctx *zero.Ctx) {
		rate, _ := strconv.ParseFloat(ctx.State["regex_matched"].([]string)[1], 64)
		if rate < 0 || rate > 1 {
			ctx.SendChain(message.Text("税率必须在0到1之间，例如0.1表示10%"))
			return
		}
		taxConfig.Lock()
		taxConfig.TaxRate = rate
		taxConfig.Unlock()
		saveConfig(engine.DataFolder() + "config.txt")
		ctx.SendChain(message.Text(fmt.Sprintf("税率已设置为%.2f%%", rate*100)))
	})

	// 设置起征点
	engine.OnRegex(`^设置起征点\s*(\d+)$`, zero.SuperUserPermission).SetBlock(true).Handle(func(ctx *zero.Ctx) {
		threshold, _ := strconv.ParseInt(ctx.State["regex_matched"].([]string)[1], 10, 64)
		if threshold < 0 {
			ctx.SendChain(message.Text("起征点不能为负数"))
			return
		}
		taxConfig.Lock()
		taxConfig.Threshold = threshold
		taxConfig.Unlock()
		saveConfig(engine.DataFolder() + "config.txt")
		ctx.SendChain(message.Text(fmt.Sprintf("起征点已设置为%d", threshold)))
	})

	// 查看税收设置
	engine.OnFullMatch("查看税收设置").SetBlock(true).Handle(func(ctx *zero.Ctx) {
		taxConfig.RLock()
		defer taxConfig.RUnlock()
		ctx.SendChain(message.Text(
			fmt.Sprintf("当前税率: %.2f%%\n起征点: %d", taxConfig.TaxRate*100, taxConfig.Threshold),
		))
	})

	// 个人税率设置
	engine.OnRegex(`^个人税率\s*\[CQ:at,qq=(\d+)\]\s*([0-9]+\.?[0-9]*)?$`, zero.SuperUserPermission).SetBlock(true).Handle(func(ctx *zero.Ctx) {
		uid, _ := strconv.ParseInt(ctx.State["regex_matched"].([]string)[1], 10, 64)
		args := ctx.State["regex_matched"].([]string)[2]

		if args == "" {
			// 查询个人税率
			rate, err := taxDB.GetUserTaxRate(uid, ctx.Event.GroupID)
			if err != nil {
				taxConfig.RLock()
				rate = taxConfig.TaxRate
				taxConfig.RUnlock()
			}
			ctx.SendChain(message.Text(fmt.Sprintf("%s的个人税率为: %.2f%%", ctx.CardOrNickName(uid), rate*100)))
		} else {
			// 设置个人税率
			rate, err := strconv.ParseFloat(args, 64)
			if err != nil || rate < 0 || rate > 1 {
				ctx.SendChain(message.Text("税率必须在0到1之间"))
				return
			}
			err = taxDB.SetUserTaxRate(uid, ctx.Event.GroupID, rate)
			if err != nil {
				ctx.SendChain(message.Text("设置个人税率失败: ", err))
				return
			}
			ctx.SendChain(message.Text(fmt.Sprintf("已设置%s的个人税率为: %.2f%%", ctx.CardOrNickName(uid), rate*100)))
		}
	})

	// 查看我的缴税记录
	engine.OnFullMatch("查看我的缴税记录").SetBlock(true).Handle(func(ctx *zero.Ctx) {
		records, err := taxDB.GetTaxRecordsByUserID(ctx.Event.UserID, 10)
		if err != nil || len(records) == 0 {
			ctx.SendChain(message.Text("您还没有缴税记录"))
			return
		}
		
		var msg strings.Builder
		msg.WriteString(fmt.Sprintf("%s的缴税记录：\n", ctx.CardOrNickName(ctx.Event.UserID)))
		for _, record := range records {
			t := time.Unix(record.TaxTime, 0)
			msg.WriteString(fmt.Sprintf("%s - 缴纳%d%s\n", t.Format("01-02 15:04"), record.TaxAmount, wallet.GetWalletName()))
		}
		ctx.SendChain(message.Text(msg.String()))
	})

	// 查看税收排行榜
	engine.OnFullMatch("查看税收排行榜").SetBlock(true).Handle(func(ctx *zero.Ctx) {
		records, err := taxDB.GetTaxRankings(ctx.Event.GroupID, 10)
		if err != nil || len(records) == 0 {
			ctx.SendChain(message.Text("暂无税收排行榜数据"))
			return
		}
		
		var msg strings.Builder
		msg.WriteString("税收排行榜（按累计缴税额排序）：\n")
		for i, record := range records {
			userName := ctx.CardOrNickName(record.UserID)
			msg.WriteString(fmt.Sprintf("%d. %s - 累计缴纳%d%s\n", i+1, userName, record.TaxAmount, wallet.GetWalletName()))
		}
		ctx.SendChain(message.Text(msg.String()))
	})

	// 手动征收个人税
	engine.OnRegex(`^手动征收个人税\s*\[CQ:at,qq=(\d+)\]`, zero.SuperUserPermission).SetBlock(true).Handle(func(ctx *zero.Ctx) {
		uid, _ := strconv.ParseInt(ctx.State["regex_matched"].([]string)[1], 10, 64)
		result := collectTaxFromUser(uid, ctx.Event.GroupID, ctx.CardOrNickName(uid))
		ctx.SendChain(message.Text(result))
	})

	// 手动征收全服税
	engine.OnFullMatch("手动征收全服税", zero.SuperUserPermission).SetBlock(true).Handle(func(ctx *zero.Ctx) {
		count, totalTax := collectTaxFromAllUsers(ctx)
		ctx.SendChain(message.Text(fmt.Sprintf("本次征收完成！共征收%d人次，总计%d%s", count, totalTax, wallet.GetWalletName())))
	})

	// 查看国库
	engine.OnFullMatch("查看国库").SetBlock(true).Handle(func(ctx *zero.Ctx) {
		treasury, err := taxDB.GetTreasuryTotal()
		if err != nil {
			treasury = 0
		}
		ctx.SendChain(message.Text(fmt.Sprintf("当前国库余额：%d%s", treasury, wallet.GetWalletName())))
	})
}

// 自动征税例程
func autoTaxRoutine() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			currentDay := now.Format("20060102")

			taxConfig.RLock()
			shouldTax := currentDay != taxConfig.LastTaxDay && now.Hour() >= autoTaxHour
			taxConfig.RUnlock()

			if shouldTax {
				logrus.Info("[tax] 开始自动征税")
				
				// 更新最后征税日期
				taxConfig.Lock()
				taxConfig.LastTaxDay = currentDay
				taxConfig.Unlock()
				
				// 异步保存配置
				go func() {
					process.SleepAboutTime(1*time.Second)
					ctrlInfo, ok := control.Lookup("tax")
					if ok {
						saveConfig(ctrlInfo.DataFolder() + "config.txt")
					}
				}()
			}
		}
	}
}

// 征收单个用户的税
func collectTaxFromUser(uid int64, groupID int64, userName string) string {
	balance := wallet.GetWalletOf(uid)
	if balance <= 0 {
		return fmt.Sprintf("%s没有%s，无需缴税", userName, wallet.GetWalletName())
	}

	taxConfig.RLock()
	threshold := taxConfig.Threshold
	taxConfig.RUnlock()

	if int64(balance) < threshold {
		return fmt.Sprintf("%s的%s余额为%d，未达到起征点%d，无需缴税", 
			userName, wallet.GetWalletName(), balance, threshold)
	}

	userTaxRate, err := taxDB.GetUserTaxRate(uid, groupID)
	if err != nil {
		taxConfig.RLock()
		userTaxRate = taxConfig.TaxRate
		taxConfig.RUnlock()
	}
	
	taxAmount := int(math.Floor(float64(balance) * userTaxRate))

	if taxAmount <= 0 {
		return fmt.Sprintf("%s无需缴税", userName)
	}

	if err := wallet.InsertWalletOf(uid, -taxAmount); err != nil {
		return fmt.Sprintf("征收%s税款失败: %v", userName, err)
	}

	// 记录缴税历史
	record := TaxRecord{
		UserID:    uid,
		GroupID:   groupID,
		TaxAmount: taxAmount,
		TaxTime:   time.Now().Unix(),
		UserName:  userName,
	}
	
	if err := taxDB.InsertTaxRecord(record); err != nil {
		logrus.Warnf("[tax] 记录缴税历史失败: %v", err)
	}

	// 记录到国库
	logEntry := TreasuryLog{
		Amount:      taxAmount,
		Operation:   "TAX_INCOME",
		Operator:    0, // 系统征收
		OpTime:      time.Now().Unix(),
		Description: fmt.Sprintf("从用户 %s(%d) 征收税收", userName, uid),
	}
	
	if err := taxDB.InsertTreasuryLog(logEntry); err != nil {
		logrus.Warnf("[tax] 记录国库日志失败: %v", err)
	}

	return fmt.Sprintf("成功向%s征收%d%s税款", userName, taxAmount, wallet.GetWalletName())
}

// 征收所有用户税
func collectTaxFromAllUsers(ctx *zero.Ctx) (int, int) {
	// 获取群成员列表
	memberListResp := ctx.GetThisGroupMemberListNoCache()
	members := memberListResp.Array()
    
    count := 0
    totalTax := 0
    
    for _, member := range members {
        uid := member.Get("user_id").Int()
        if uid == 0 {
            continue
        }
        
        balance := wallet.GetWalletOf(uid)
        if balance <= 0 {
            continue
        }

        taxConfig.RLock()
        threshold := taxConfig.Threshold
        taxConfig.RUnlock()

        if int64(balance) < threshold {
            continue
        }

        userTaxRate, err := taxDB.GetUserTaxRate(uid, ctx.Event.GroupID)
        if err != nil {
            taxConfig.RLock()
            userTaxRate = taxConfig.TaxRate
            taxConfig.RUnlock()
        }

        taxAmount := int(math.Floor(float64(balance) * userTaxRate))

        if taxAmount <= 0 {
            continue
        }

        if err := wallet.InsertWalletOf(uid, -taxAmount); err != nil {
            continue
        }

        // 记录缴税历史
        userName := ctx.CardOrNickName(uid)
        record := TaxRecord{
            UserID:    uid,
            GroupID:   ctx.Event.GroupID,
            TaxAmount: taxAmount,
            TaxTime:   time.Now().Unix(),
            UserName:  userName,
        }
        
        if err := taxDB.InsertTaxRecord(record); err != nil {
            continue
        }

        // 记录到国库
        logEntry := TreasuryLog{
            Amount:      taxAmount,
            Operation:   "TAX_INCOME",
            Operator:    ctx.Event.UserID,
            OpTime:      time.Now().Unix(),
            Description: fmt.Sprintf("手动征收 - 从用户 %s(%d) 征收税收", userName, uid),
        }
        
        if err := taxDB.InsertTreasuryLog(logEntry); err != nil {
            continue
        }
        
        count++
        totalTax += taxAmount
    }

	return count, totalTax
}

// 保存配置
func saveConfig(path string) {
	taxConfig.RLock()
	data := fmt.Sprintf("%.4f,%d,%s", taxConfig.TaxRate, taxConfig.Threshold, taxConfig.LastTaxDay)
	taxConfig.RUnlock()
	
	_ = binary.WriteFile([]byte(data), path)
}

// 加载配置
func loadConfig(path string) {
	if !file.IsExist(path) {
		// 如果配置不存在，使用默认值
		return
	}

	data, err := file.GetLazyData(path, false, true)
	if err != nil {
		return
	}

	parts := strings.Split(string(data), ",")
	if len(parts) >= 2 {
		taxConfig.Lock()
		defer taxConfig.Unlock()
		
		if rate, err := strconv.ParseFloat(parts[0], 64); err == nil {
			taxConfig.TaxRate = rate
		}
		if threshold, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
			taxConfig.Threshold = threshold
		}
	}
	if len(parts) >= 3 {
		taxConfig.Lock()
		taxConfig.LastTaxDay = parts[2]
		taxConfig.Unlock()
	}
}
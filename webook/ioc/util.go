package ioc

import "gitee.com/geekbang/basic-go/webook/pkg/util"

func InitUuidFn() util.UuidFn {
	return util.NewUuidFn()
}

// func InitCpuUsageFetcher() {
// 	return
// }

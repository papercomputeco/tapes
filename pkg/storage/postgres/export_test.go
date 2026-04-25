package postgres

func ToMigrateDSNForTest(dsn string) string {
	return toMigrateDSN(dsn)
}

func InterfaceInt32ForTest(v any) int32 {
	return interfaceInt32(v)
}

func InterfaceInt64ForTest(v any) int64 {
	return interfaceInt64(v)
}

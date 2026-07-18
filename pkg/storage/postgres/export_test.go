package postgres

func ToMigrateDSNForTest(dsn string) string {
	return toMigrateDSN(dsn)
}

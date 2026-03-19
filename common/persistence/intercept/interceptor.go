package intercept

type PersistenceInterceptor func(methodName string, fn func() (any, error), params ...any) error

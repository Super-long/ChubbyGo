package Connect

// -------------------------------------------

type ErrorInConnectAll int8 // 特用于connectAll的返回值 因为有三种错误 无法用bool表示

const (
	time_out = iota
	http_error
)

func (err ErrorInConnectAll) Error() string {
	var ans string
	switch err {
	case time_out:
		ans = "Connection to a target server greater than or equal to timeout."
		break
	case http_error:
		ans = "rpc.DialHTTP return HTTP error."
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInStartClient int8	// 特用于StartServer的返回值 因为有三种错误 无法用bool表示

const (
	parser_error = iota
	connect_error
)

func (err ErrorInStartClient) Error() string{
	var ans string
	switch err {
	case parser_error:
		ans = "Error parsing configuration file."
		break
	case connect_error:
		ans = "Connection error, Refer to ErrorInConnectAll for specific error types."
	default:

	}
	return ans
}

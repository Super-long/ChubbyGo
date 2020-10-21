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
		break
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInStartServer int8	// 特用于StartServer与StartClient的返回值 因为有四种错误 无法用bool表示

const (
	parser_error = iota
	connect_error
	Listener_error
)

func (err ErrorInStartServer) Error() string{
	var ans string
	switch err {
	case parser_error:
		ans = "Error parsing configuration file."
		break
	case connect_error:
		ans = "Connection error, Refer to ErrorInConnectAll for specific error types."
		break
	case Listener_error:
		ans = "Listener error, Guess that the init function of read_c/s_config.go is not started at the call site."
		break
	default:

	}
	return ans
}

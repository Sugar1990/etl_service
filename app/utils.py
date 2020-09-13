def success_res(input_message,data={}):
    res= {
        "code": 1,
        "massage": input_message,
        "data": data
    }
    return res

def fail_res(msg="request fail"):
    return {"code": 0, "msg": msg}

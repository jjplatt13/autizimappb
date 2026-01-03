def parse_device_type(user_agent: str) -> str:
    ua = user_agent.lower() if user_agent else ""

    if "iphone" in ua or "ipad" in ua:
        return "ios_mobile"
    if "android" in ua:
        return "android_mobile"
    if "windows" in ua:
        return "windows_desktop"
    if "macintosh" in ua or "mac os" in ua:
        return "mac_desktop"
    if "linux" in ua:
        return "linux_desktop"

    return "unknown"

def app_redis_key(name: str, *, namespace: str | None = None) -> str:
    """Build a redis key segment with optional app namespace."""
    cleaned_name = name.strip().strip(":")
    cleaned_namespace = (namespace or "").strip().strip(":")
    if not cleaned_namespace:
        return cleaned_name
    return f"{cleaned_namespace}:{cleaned_name}"

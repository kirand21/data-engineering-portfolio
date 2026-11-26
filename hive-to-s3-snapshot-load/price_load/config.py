from datetime import datetime

REQUIRED = ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD")


def validate_date(value):
    datetime.strptime(value, "%Y-%m-%d")
    return value


def jdbc_options_from_env(env, table):
    missing = [name for name in REQUIRED if not env.get(name)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    return {
        "url": "jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}".format(**env),
        "dbtable": table,
        "user": env["PG_USER"],
        "password": env["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "20000",
    }

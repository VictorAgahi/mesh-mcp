import redis

r = redis.Redis()


def test_publish():
    r.publish("test.only", "x")

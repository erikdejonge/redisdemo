# coding=utf-8
"""
Testscript for redis
"""
import unittest
import random
import time
import redis

from multiprocessing import Pool, Process

REDISHOST = '127.0.0.1'


# noinspection PyUnusedLocal
def return_redis_mylist(counter):
    """
    pop one item from the beginning of the list
    """
    global REDISHOST

    # decode responses is for utf8 return values
    remoterediscon = redis.Redis(host=REDISHOST, decode_responses=True)
    return remoterediscon.lpop("mylist")


class RedisTest(unittest.TestCase):
    """
    redis
    """
    rcon = None

    def setUp(self):
        """
        setUp
        """
        global REDISHOST
        self.REDISHOST = REDISHOST

        # make connection attribute, decode responses is for utf8 return values
        self.rcon = redis.Redis(host=self.REDISHOST, decode_responses=True)

    def test_make_conn(self):
        """
        connection should be made by now
        """
        self.assertIsNotNone(self.rcon)

    def test_scalar_string_no_driver_decoding(self):
        """
        setting and getting of a single string
        """

        # without decode_responses we get bytestrings
        self.rcon = redis.Redis(host=self.REDISHOST)

        # in case of failed tests
        self.rcon.delete("mystr")
        thestr = "Hello world! 🙂"
        self.rcon.set("mystr", thestr)
        mystr = self.rcon.get("mystr")

        # Carefull: redis returns binary strings (not encoded)
        self.assertNotEqual(thestr, mystr)

        # decode to the default (utf8)
        mystr = mystr.decode()

        # now they should be equal
        self.assertEqual(thestr, mystr)

    def test_scalar_string(self):
        """
        setting and getting of a single string
        """

        # in case of failed tests
        self.rcon.delete("mystr")
        thestr = "Hello world! 🙂"
        self.rcon.set("mystr", thestr)
        mystr = self.rcon.get("mystr")

        # now they should be equal
        self.assertEqual(thestr, mystr)

    def test_scalar_int(self):
        """
        set and get integer
        """

        # in case of failed tests
        self.rcon.delete("somenumber")

        # a random int as the value
        randomnumber = random.randint(-100, 100)
        self.rcon.set("somenumber", randomnumber)

        # Be carefull all data is stored as a string
        somenumber = self.rcon.get("somenumber")
        self.assertNotEqual(somenumber, randomnumber)

        # cast the string to an int
        somenumber = int(somenumber)
        self.assertEqual(somenumber, randomnumber)

        # delete key
        self.rcon.delete("somenumber")

        # shoyld return None now
        somenumber = self.rcon.get("somenumber")
        self.assertIsNone(somenumber)

    def test_counters(self):
        """
        set a counter (atomic)
        """

        # make a counter by increasing it with startnumber
        self.rcon.delete("mycounter")
        self.rcon.incr("mycounter", 2)

        self.assertEqual(int(self.rcon.get("mycounter")), 2)

        self.rcon.incr("mycounter", 2)
        self.rcon.incr("mycounter", 4)

        self.assertEqual(int(self.rcon.get("mycounter")), 8)

    def test_list(self):
        """
        test a list, this is like a global list and atomic, multiple programs can pop this
        """
        self.rcon.delete("mylist")

        # first in last out list
        self.rcon.lpush("mylist", "🙂")
        self.rcon.lpush("mylist", "world!")
        self.rcon.lpush("mylist", "Hello")

        self.assertEqual(self.rcon.llen("mylist"), 3)

        # get the list as a whole
        # getting it as scalar throws exception

        with self.assertRaises(redis.exceptions.ResponseError):
            self.rcon.get("mylist")

        # left pop the list untill its empty
        mylist = []
        spart = self.rcon.lpop("mylist")

        while spart:
            mylist.append(spart)
            spart = self.rcon.lpop("mylist")

        mystring = " ".join(mylist)
        self.assertEqual(mystring, "Hello world! 🙂")

    def test_list_smp(self):
        """
        pop a list from multiple processes
        """
        self.rcon.delete("mylist")

        # a local and a redis with 100 random numbers
        verifylist = []

        for i in range(0, 100):
            rnum = random.randint(0, 100)
            verifylist.append(rnum)
            self.rcon.lpush("mylist", rnum)

        # call the popitem method a 100 times
        pool = Pool(processes=4)
        returnedlist = pool.map(return_redis_mylist, range(0, 100))
        returnedlist = sorted([int(val) for val in returnedlist])

        # sort both lists, should be the same
        verifylist.sort()
        self.assertEqual(returnedlist, verifylist)

    def test_hash(self):
        """
        Hash items are like dictionaries
        """
        self.rcon.delete("mydict")

        # set the dict (dictname, key value)
        self.rcon.hset("mydict", "naam", "adisor")
        self.rcon.hset("mydict", "city", "rotterdam")

        # the redis driver returns a dict
        mydict = {"naam": "adisor",
                  "city": "rotterdam"}

        self.assertEqual(self.rcon.hgetall("mydict"), mydict)

    def test_set(self):
        """
        a set is a unique list like in python
        """
        self.rcon.delete("myset")
        myset = set()
        for i in [1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5]:
            myset.add(i)
            self.rcon.sadd("myset", i)

        # seeems to return it as a list
        rmyset = sorted([int(val) for val in self.rcon.smembers("myset")])
        mysetlist = list(myset)
        self.assertEqual(mysetlist, rmyset)

    def test_pubsub(self):
        """
        """

        self.rcon.delete("mylist")

        def pub(myredis):
            """
            this is a publisher, can be whatever event, could for example be a click somewhere in javascript
            """
            for n in range(5):
                myredis.publish('myevents', 'the number is %d' % n)
                time.sleep(0.01)

        def sub(myredis, name):
            """
            This is the subscriber method, this method is waiting around in a Process
            could be something like a cronjob, or just a good way to propate an event
            instead of functions calling each other, since that could lead to tight coupling.
            A microservice architecture would be a perfect fit, mini webservices working together
            with redis as the working memory.
            """
            pubsub = myredis.pubsub()

            # can subscribe to multiple events
            pubsub.subscribe(['myevents', 'myotherevents'])

            # an item is a dict with the following keys, type, pattern, channel, data
            # in this case it looks something like this
            # {'type': 'message',
            #  'pattern': None,
            #  'channel': 'myevents',
            #  'data': 'the number is 4'}
            for item in pubsub.listen():

                # push the received item in a shared list on redis.
                myredis.lpush("mylist", str(name) + ": " + str(item['data']))

        # start the publisher (user clicking on a button or program notifying that it's done)
        p0 = Process(target=pub, args=(self.rcon,))
        p0.start()

        # start two subscribers
        p1 = Process(target=sub, args=(self.rcon, 'reader1'))
        p1.start()

        # the just catch the data and push it in a shared list
        p2 = Process(target=sub, args=(self.rcon, 'reader2'))
        p2.start()

        # wait a little while, there is a small delay between the events
        time.sleep(0.2)

        # process are killed now, in real life they could be still running for example cronjobs?
        p0.terminate()
        p1.terminate()
        p2.terminate()

        # loop the redislist and make a normal list
        mylist = []
        while self.rcon.llen('mylist') != 0:
            mylist.append(self.rcon.lpop("mylist"))

        mylist.sort()
        listtocheck = ['reader1: 1', 'reader1: 2', 'reader1: the number is 1', 'reader1: the number is 2', 'reader1: the number is 3', 'reader1: the number is 4', 'reader2: 1', 'reader2: 2', 'reader2: the number is 1', 'reader2: the number is 2', 'reader2: the number is 3', 'reader2: the number is 4']
        self.assertEqual(mylist, listtocheck)


def main():
    """
    run unittests
    """
    unittest.main()


if __name__ == '__main__':
    main()

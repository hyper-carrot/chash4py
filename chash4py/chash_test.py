'''
Created on 2013-01-11

@author: linhao
'''

import unittest
import chash
import time
import types
import datetime,time
import random
import string
import collections

class Test(unittest.TestCase):

    __functional_test = True

    __benchmark_test = True

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFuncs(self):
        if not self.__functional_test:
            print "The functional test 'testFuncs' is ignored."
            return
        # test function 'is_seq'
        self.assertTrue(chash.is_seq([1,2,3]))
        self.assertTrue(chash.is_seq(set([1,2,3])))
        self.assertTrue(chash.is_seq((1,2,3)))
        self.assertFalse(chash.is_seq("abc"))
        self.assertFalse(chash.is_seq(123))
        self.assertFalse(chash.is_seq({1: "A"}))

##        bytes2 = [86, 133, 42, 84, 86, 209, 176, 158, 30, 177, 28, 12, 163, 157, 143, 188, 230, 72, 1, 6]
##    	i = 0
##    	print "Ketama Number 1-1:", (bytes2[3+i*4]&0xFF), ",", (bytes2[3+i*4]&0xFF)<<24
##    	print "Ketama Number 1-2:", (bytes2[2+i*4]&0xFF), ",", (bytes2[2+i*4]&0xFF)<<16
##    	print "Ketama Number 1-3:", (bytes2[1+i*4]&0xFF), ",", (bytes2[1+i*4]&0xFF)<<8
##    	print "Ketama Number 1-4:", bytes2[i*4]&0xFF, ",", bytes2[i*4]&0xFF
##
##        n = 133
##        print n<<1, n<<2, n<<4, n<<5, n<<6, n<<7, n<<8

        # test function 'get_hash_numbers'
        hash_numbers = chash.get_hash_numbers("127.0.0.1:8080")
        print "string: 127.0.0.1:8080, hash numbers:", hash_numbers
        self.assertEqual(chash.get_hash_numbers("127.0.0.1:8080"), [86, 133, 42, 84, 86, 209, 176, 158, 30, 177, 28, 12, 163, 157, 143, 188, 230, 72, 1, 6])

        # test function 'get_ketama_numbers'
        ketama_numbers = chash.get_ketama_numbers("127.0.0.1:8080")
        print "string: 127.0.0.1:8080, ketama numbers:", ketama_numbers
        self.assertSequenceEqual(ketama_numbers, [1412072790, 2662388054L, 203206942, 3163528611L])

        # test function 'get_hash_key'
        key_hash =chash.get_hash_for_key("abc")
        print "key: abc, key hash:", key_hash
        self.assertEqual(key_hash, 1604963272)

    def testNodeDict(self):
        nd = chash.NodeDict({6:"G",7:"H",8:"I",1:"A",2:"B",3:"C",4:"E",5:"F"})
        nd_cp = nd.copy()
        self.assertEqual(str(nd), str(nd_cp))
        count = 1
        for n in nd_cp.iteritems():
            self.assertEqual(n[0], count)
            count += 1
            self.assertEqual(n[1], nd[n[0]])
        d1 = {6:"G1",7:"H1", 9:"J"}
        nd_cp.update(d1)
        self.assertEqual(nd_cp[6], "G1")
        self.assertEqual(nd_cp[7], "H1")
        self.assertEqual(nd_cp[9], "J")
        filtered_dict = {k:v for k, v in nd_cp.items() if v != "A"}
        self.assertEqual(len(filtered_dict) + 1, len(nd_cp))
        nd_cp.delete({9:"J"})
        self.assertIsNone(nd_cp[9])
        nd_cp.update({11:"L"})
        self.assertEqual(nd_cp[11], "L")
        self.assertEqual(nd_cp.to_node(10), 11)
        self.assertEqual(nd_cp.to_node(8), 8)
        self.assertEqual(nd_cp.to_node(15), 1)
        self.assertEqual(nd_cp.to_node(0), 1)

    def testHashRing(self):
        if not self.__functional_test:
            print "The functional test 'testHashRing' is ignored."
            return
        hosts = ("10.11.156.71:2181", "10.11.5.145:2181", "10.11.5.164:2181", "192.168.106.63:2181", "192.168.106.64:2181")
        key = "abc"
        print "All hosts: {0}".format(hosts)
        print "Creat Hash Ring ..."
        ring = chash.HashRing(check_func=None)
        t1 = ring.get_target(key)
        print "The target of key '{0}' is '{1}'. (1)".format(key, t1)
        self.assertIsNone(t1)
        print "Add targets ({0})...".format(hosts)
        ring.add_targets(hosts)
        t2 = ring.get_target(key)
        print "The target of key '{0}' is '{1}'. (2)".format(key, t2)
        self.assertIsNotNone(t2)
        print "Remove target '{0}'".format(t2)
        ring.remove_targets(t2)
        t3 = ring.get_target(key)
        print "The target of key '{0}' is '{1}'. (3)".format(key, t3)
        self.assertIsNotNone(t3)
        self.assertNotEqual(t2, t1)
        print "Remove targets '{0}'".format(hosts)
        ring.remove_targets(hosts)
        t4 = ring.get_target(key)
        print "The target of key '{0}' is '{1}'. (4)".format(key, t4)
        self.assertIsNone(t4)
        ring.destroy()

    def testHashRingForBenchmark(self):
        debugTag = False
        if not self.__benchmark_test:
            print "The benchmark test 'testHashRingPerf' is ignored."
            return
        hosts = ("10.11.156.71:2181", "10.11.5.145:2181", "10.11.5.164:2181", "192.168.106.63:2181", "192.168.106.64:2181")
        print "All hosts: {0}".format(hosts)
        print "Creat Hash Ring ..."
        ring = chash.HashRing(check_func=None)
        print "Add targets ({0})...".format(hosts)
        ring.add_targets(hosts)

        sample_str_az = "abcdefghijklmnopqrstuvwxyz"
        sample_str = sample_str_az + sample_str_az.upper() + " ~!@#$%^&*()_+-="
        loopNumbers = [10000, 20000, 50000, 100000, 200000, 300000, 400000, 500000]
        for loopNumber in loopNumbers:
            target_count_dict = collections.OrderedDict({h:0 for h in hosts})
            keys = list()
            for i in range(0, loopNumber):
                keys.append(string.join(random.sample(sample_str, 10)).replace(" ", ""))
            starttime = datetime.datetime.now()
            for i in range(0, loopNumber):
                key = keys[i]
                t = ring.get_target(key)
                if debugTag:
                    print "The target of key '{0}' is '{1}'. ({2})".format(key, t, i)
                self.assertIsNotNone(t)
                target_count_dict[t] += 1
            endtime = datetime.datetime.now()
            difftime = (endtime - starttime)
            totalCost = difftime.seconds * 1000 * 1000 + difftime.microseconds
            eachCost = totalCost / float(loopNumber)
            print "Benchmark Result (loopNumber={0}) - Total cost (microsecond): {1}, Count: {2}, Each cost (microsecond): {3}.".format(loopNumber, totalCost, loopNumber, eachCost)
            print "The target count dict (loopNumber={0}): {1}".format(loopNumber, target_count_dict)
        ring.destroy()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
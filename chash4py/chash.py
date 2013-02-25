'''
Created on 2013-01-10

@author: linhao
'''
import hashlib
from itertools import *
import threading
import bisect
from chash4py.timer import RepeatTimer
from chash4py.ilogging import get_logger

def is_seq(obj):
    return isinstance(obj, list) | isinstance(obj, set) | isinstance(obj, tuple)

def get_hash_numbers(str):
    digest = hashlib.sha1(str).hexdigest()
    step = 2
    h_list = [digest[i:i+step] for i in xrange(0, len(digest), step)]
    h_ilist =[]
    for h in h_list:
        h_ilist.append(int(h, 16))
    return h_ilist

def get_ketama_numbers(key):
    digest = get_hash_numbers(key)
    numbers = []
    for i in range(0, 4):
        h = (digest[3 + i * 4] & 0xFF) << 24 \
            | (digest[2 + i * 4] & 0xFF) << 16 \
            | (digest[1 + i * 4] & 0xFF) << 8 \
            | digest[i * 4] & 0xFF
        numbers.append(h)
    return numbers

def get_hash_for_key(str):
    h = 0
    h_list = get_ketama_numbers(str)
    h = sum(h_list)
    return h / len(h_list)

class HashRingError(Exception):
    """A error class for Hash Ring"""
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class NodeDict():
    """
    A non-thread-safe & ring-like & dict-like class
    """
    def __init__(self, contents={}):
        if not isinstance(contents, dict):
            raise HashRingError("The contents must be a dict, but its type: {0}.".format(type(contents)))
        self._list = sorted(contents)
        self._dict = contents

    def __iter__(self):
        return (k for k in self._list)

    def __contains__(self, k):
        try:
            self._dict[k]
        except KeyError:
            return False
        else:
            return True

    def __eq__(self, other):
        if isinstance(other, NodeDict):
            return False
        if self._dict != other._dict:
            return False
        if self._list != other._list:
            return False
        return True

    def __len__(self):
        if not self._list:
            return 0
        return len(self._list)

    def __getitem__(self, k):
        if k:
            if k in self._dict:
                return self._dict[k]
        return None

    def __delitem__(self, k):
        if k in self._dict:
            del self._dict[k]
        i = bisect.bisect_left(self._list, k)
        if i < len(self._list):
            del self._list[i]

    def __setitem__(self, k, v):
        self._dict[k] = v
        if k not in self._list:
            bisect.insort_left(self._list, k)

    def __str__(self):
        l = []
        for k in self._list:
            v = None
            if k in self._dict: v = self._dict[k]
            l.append(str(k) + ": " + str(v))
        return "NodeDict({" + ", ".join(l) + "})"

    def update(self, contents):
        if not isinstance(contents, dict):
            raise HashRingError("The contents must be a dict, but its type: {0}.".format(type(contents)))
        self._dict.update(contents)
        new_list = [k for k in contents.iterkeys() if k not in self._list]
        for n in new_list: bisect.insort_left(self._list, n)

    def delete(self, contents):
        if not isinstance(contents, dict):
            raise HashRingError("The contents must be a dict, but its type: {0}.".format(type(contents)))
        for k in contents.iterkeys():
            del self[k]

    def copy(self):
        return NodeDict(self._dict.copy())

    def to_node(self, key):
        if not self._list:
            return None
        i = bisect.bisect_left(self._list, key)
        if i == len(self._list):
            return self._list[0]
        else:
            return self._list[i]

    def get(self, key, default=None):
        try:
            return self._dict[key]
        except KeyError:
            return default

    def iterkeys(self):
        return iter(self._list)

    def itervalues(self):
        for key in self._list:
            yield self._dict[key]

    def iteritems(self):
        for key in self._list:
            yield (key, self._dict[key])

    def keys(self):
        return self._list.copy()

    def items(self):
        return [(key, self._dict[key]) for key in self._list]

    def values(self):
        return [self._dict[key] for key in self._list]


class HashRing:

    # NodeDict (sorted by key): int -> string
    __node_target_dict = None

    # dict: string -> list(int)
    __target_nodes_dict = None

    __change_event = None

    __check_func = None

    __invalid_target_set = None

    __timer = None

    __builded = None

    __destroyed = None

    def __init__(self, check_func, shadow_number=1000):
        self.__change_event = threading.Event()
        self.__change_event.set()
        self.__builded = threading.Event()
        self.__destroyed = threading.Event()
        self.build(check_func, shadow_number)

    def build(self, check_func, shadow_number=1000):
        if self.__builded.isSet():
            get_logger().info("Please destroy hash ring before rebuilding.")
            return
        self.__change_event.wait()
        self.__change_event.clear()
        try:
            self.__builded.set()
            self.__check_func = check_func
            if shadow_number > 0:
                self._shadow_number = shadow_number
            else:
                self._shadow_number = 1000
            self.__node_target_dict = NodeDict()
            self.__target_nodes_dict = {}
            self.start_checking()
            self.__destroyed.clear()
            get_logger().info("The hash ring is builded.")
        finally:
            self.__change_event.set()

    def destroy(self):
        if self.__destroyed.isSet():
            get_logger().info("The hash ring has been destroyed. IGNORE the destroy operation.")
            return
        self.__change_event.wait()
        self.__change_event.clear()
        try:
            self.__destroyed.set()
            self.stop_checking()
            self.__node_target_dict = None
            self.__target_nodes_dict = None
            self.__builded.clear()
            get_logger().info("The hash ring is destroyed.")
        finally:
            self.__change_event.set()

    def start_checking(self):
        if not self.__check_func:
            get_logger().info("The check function is None. IGNORE the checking operation.")
        else:
            if not self.__timer:
                self.__invalid_target_set = set([])
                self.__timer = RepeatTimer(2, self.check)
            get_logger().info("Starting node checking timer...")
            self.__timer.start()

    def check(self):
        if not self.__check_func:
            get_logger().debug("The check function is None. IGNORE checking.")
            return
        get_logger().debug("Checking nodes in hash ring...")
        self.__change_event.wait()
        targets = self.__target_nodes_dict.keys()
        for t in targets:
            result = self.__check_func(t)
            if not result:
                get_logger().info("The target '{0}' is INVALID. Delete it from hash ring.".format(t))
                self.__remove_target(t)
                self.__invalid_target_set = self.__invalid_target_set | set([t])
        get_logger().debug("Checking the nodes which have been added but invalid...")
        for it in self.__invalid_target_set:
            result = self.__check_func(it)
            if result:
                 get_logger().info("The invalid target '{0}' is OK. Add it to hash ring.".format(it))
                 self.__add_target(it)
                 self.__invalid_target_set = self.__invalid_target_set - set([it])

    def stop_checking(self):
        if self.__timer:
            self.__timer.cancel()
        self.__timer = None

    def __add_target(self, target):
        self.__change_event.wait()
        self.__change_event.clear()
        try:
            target_shadows = []
            target_nodes = []
            current_node_target_dict = {}
            for i in range(0, self._shadow_number):
                target_shadows.append("%s-%s" % (target, i))
            for target_shadow in target_shadows:
                numbers = get_ketama_numbers(target_shadow)
                for number in numbers:
                    current_node_target_dict[number] = target
                    target_nodes.append(number)
            self.__target_nodes_dict[target] = target_nodes
            self.__node_target_dict.update(current_node_target_dict)
            get_logger().info("The target '{0}' is added successfully. The number of node: {1}.".format(target, len(target_nodes)))
            get_logger().debug("The nodes of target '{0}' is {1}.".format(target, target_nodes))
        finally:
            self.__change_event.set()

    def __remove_target(self, target):
        self.__change_event.wait()
        self.__change_event.clear()
        try:
            if self.__node_target_dict:
                invalid_node_target_dict = {k:v for k, v in self.__node_target_dict.items() if v == target}
                self.__node_target_dict.delete(invalid_node_target_dict)
            if self.__target_nodes_dict:
                if target in self.__target_nodes_dict:
                    del self.__target_nodes_dict[target]
            get_logger().info("The target '{0}' is removed successfully.".format(target))
        finally:
            self.__change_event.set()

    def parse_targets(self, targets):
        target_set = None
        if is_seq(targets):
            target_set = set(targets)
        elif isinstance(targets, str):
            target_set = set([targets])
        else:
            raise HashRingError("The type of parameter 'targets' is UNSUPPORTED! %s" % type(targets))
        return target_set

    def add_targets(self, targets):
        target_set = self.parse_targets(targets)
        for target in target_set:
            self.__add_target(target)

    def remove_targets(self, targets):
        target_set = self.parse_targets(targets)
        for target in target_set:
            self.__remove_target(target)

    def get_target(self, value):
        if not value:
            return None
        self.__change_event.wait()
        h = get_hash_for_key(str(value))
        node = self.__node_target_dict.to_node(h)
        target = None
        if node in self.__node_target_dict:
            target = self.__node_target_dict[node]
        get_logger().debug("The target of key '{0}' ({1}) is '{2}' ({3}).".format(value, h, target, node))
        return target


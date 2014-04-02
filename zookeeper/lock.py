'''Some basic implementations of lock primitive in Zookeeper.
The python-zk/kazoo is used as python zk library. It should be mentioned that 
kazoo already has itself implementation of lock in the 'recipe' module, 
however this copy of work only uses the basic level api of kazoo and its 
purpose targets at principle illustration.
'''

import sys
import random
import time
import threading
import logging

from kazoo.client import KazooClient
import kazoo

def do_something(): 
    T = 3
    time.sleep(random.randint(1, T))

class PollingZKLock(): 
    '''A polling style implementation of mutex primitive using ZK API.
    '''
    def __init__(self, zk): 
        self.zk = zk

    def acquire(self): 
        while True: 
            try: 
                self.zk.create('/lock', '', ephemeral=True)
                break
            except kazoo.exceptions.NodeExistsError: 
                # wait and then retry.
                time.sleep(0.1)
            except Exception as e: 
                sys.stderr.write('Unhandled Exception: ' + str(e))

    def release(self): 
        self.zk.delete('/lock')

class EventZKLock(): 
    '''A event based ZK Lock implementation.
    '''
    def __init__(self, zk): 
        self.zk = zk
        self.cond_mutex = threading.Condition()

    def on_node_change(self, event): 
        if event.type == kazoo.protocol.states.EventType.DELETED: 
            print 'lock node has been cleared'
            self.cond_mutex.acquire()
            self.cond_mutex.notify()
            # the waiting thread cannot resume from 'wait()', 
            # util mutex is released by caller.
            self.cond_mutex.release() 

    def acquire(self): 
        '''event-based acquire.
        '''
        self.cond_mutex.acquire()
        while True: 
            try: 
                stat = self.zk.exists('/lock', watch=self.on_node_change)
                if not stat: 
                    self.zk.create('/lock', '', ephemeral=True)
                    break # acquire succeed
                else: 
                    self.cond_mutex.wait()
                    continue # retry acquire
            except kazoo.exceptions.NodeExistsError: 
                self.cond_mutex.wait() # then, retry acquire
            except Exception as e: 
                sys.stderr.write('Unhandled Exception: ' + str(e))
                
        self.cond_mutex.release()

    def release(self): 
        self.zk.delete('/lock')

if __name__ == '__main__': 
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()
    lock = EventZKLock(zk)
    
    while True: 
        try: 
            lock.acquire()
            print 'got the lock, and do something exclusively.'
            do_something() # do something in critical area.

            lock.release()
            do_something() # do somethine out of critical area.
        except KeyboardInterrupt: 
            break
        except Exception as e: 
            sys.stderr.write('Unhandled Exception: ' + str(e))

    zk.stop()

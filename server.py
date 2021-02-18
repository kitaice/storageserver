from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import random, time, threading, queue, errno

from threading import Timer
import time
from collections import defaultdict
import logging

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

def readconfig(config, servernum):
    # global host, port, serverlist
    """Reads cofig file"""

    fd = open(config, 'r')
    l = fd.readline()
    serverlist = {}
    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            print(hostport, i)
            # peer = xmlrpc.client.ServerProxy("http://" + hostport)
            serverlist[i] = hostport
            # all peer server connections in a

    return maxnum, host, port, serverlist

  # Reads the config file and return host, port and store list of other servers


class Surfstore():
    def __init__(self, config, servernum):
        fd = open(config, 'r')
        l = fd.readline()
        self.serverlist = {}
        self.maxnum = int(l.strip().split(' ')[1])
        if servernum >= self.maxnum or servernum < 0:
            raise Exception('Server number out of range.')
        d = fd.read()
        d = d.splitlines()

        for i in range(len(d)):
            hostport = d[i].strip().split(' ')[1]
            if i == servernum:
                host = hostport.split(':')[0]
                port = int(hostport.split(':')[1])
            else:
                print(hostport, i)
                peer = xmlrpc.client.ServerProxy("http://" + hostport)
                self.serverlist[i] = peer
                # all peer server connections in a

        self.APPEND_ENTRY_RECEIVED = 'APPEND_ENTRY_RECEIVED'
        self.SEND_HEART_BEAT = 'SEND_HEART_BEAT'
        self.RESET_ELEC_TIMEOUT = 'RESET_ELEC_TIMEOUT'
        self.ELECTION = 'ELECTION'
        self.ELEC_FAIL_RELECT = 'ELEC_FAIL_RELECT'
        self.LEADER = "Leader"
        self.FOLLOWER = "Follower"
        self.CANDIDATE = "Candidate"
        self.state = self.FOLLOWER
        self.electionTimeout = 1.5 * random.random() + 1
        self.heartbeatTimeout = 0.2

        self.votedFor = None
        self.currentTerm = 0
        self.vote = 0
        self.lock = threading.Lock()

        self.elec_timer_messages = queue.Queue()
        self.controller_messages = queue.Queue()
        self.last_update = time.time()
        self.lifeCount = 0

        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = []
        self.matchIndex = []

        self.crashState = False
        self.currentIndex = 0
        self.hashmap = dict()
        self.fileinfomap = dict()
        self.id = servernum
        self.log = []

        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(self.ping,"surfstore.ping")
        server.register_function(self.getblock,"surfstore.getblock")
        server.register_function(self.putblock,"surfstore.putblock")
        server.register_function(self.hasblocks,"surfstore.hasblocks")
        server.register_function(self.getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(self.updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(self.isLeader,"surfstore.isLeader")
        server.register_function(self.crash,"surfstore.crash")
        server.register_function(self.restore,"surfstore.restore")
        server.register_function(self.isCrashed,"surfstore.isCrashed")
        server.register_function(self.requestVote,"surfstore.requestVote")
        server.register_function(self.appendEntries,"surfstore.appendEntries")
        server.register_function(self.requestVoteRPC, "surfstore.requestVoteRPC")
        server.register_function(self.appendEntriesRPC, "surfstore.appendEntriesRPC")
        server.register_function(self.tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        controller_t = threading.Thread(target=self.controller, args=())
        controller_t.start()
        election_timer_t = threading.Thread(target=self.election_timer, args=())
        election_timer_t.start()
        hbeat_timer_t = threading.Thread(target=self.heart_beat_timer, args=())
        hbeat_timer_t.start()

        server.serve_forever()


    def gethash(self,block):
        hash_value = hashlib.sha256(block).hexdigest()
        return hash_value

    # A simple ping, returns true
    def ping(self):
        """A simple ping method"""
        print("Ping()")
        return True

    # Gets a block, given a specific hash value
    def getblock(self,h):
        """Gets a block"""
        print("GetBlock(" + h + ")")

        # blockData = bytes(4)
        return self.hashmap[h]

    # Puts a block
    def putblock(self,b):
        """Puts a block"""
        print("PutBlock()")
        h = gethash(b.data)
        self.hashmap[h] = b.data
        return True

    # Given a list of hashes, return the subset that are on this server
    def hasblocks(self,hashlist):
        """Determines which blocks are on this server"""
        print("HasBlocks()")
        exist_block = []
        for block in hashlist:
            if block in self.hashmap:
                exist_block.append(block)

        return exist_block

    # Retrieves the server's FileInfoMap
    def getfileinfomap(self):
        """Gets the fileinfo map"""
        print("GetFileInfoMap()")
        time.sleep(self.heartbeatTimeout)
        return self.fileinfomap

    # Update a file's fileinfo entry
    def updatefile(self,filename, version, hashlist):
        """Updates a file's fileinfo entry"""
        if self.state != self.LEADER or self.crashState:
            raise Exception('not leader or crash')
        print("UpdateFile("+filename+")")
        while self.state == self.LEADER and self.lifeCount < int(0.5 * (len(self.serverlist) + 1))+1:
            continue
        if self.state != self.LEADER:
            raise Exception('not leader')
        self.fileinfomap[filename] = {}
        self.fileinfomap[filename]['version'] = version
        self.fileinfomap[filename]['hashlist'] = hashlist

        # fileinfomap[filename] = [version, hashlist]
        return True

    # PROJECT 3 APIs below

    # Queries whether this metadata store is a leader
    # Note that this call should work even when the server is "crashed"
    def isLeader(self):
        """Is this metadata store a leader?"""
        print("IsLeader()")
        # return True
        return self.state == self.LEADER

    # "Crashes" this metadata store
    # Until Restore() is called, the server should reply to all RPCs
    # with an error (unless indicated otherwise), and shouldn't send
    # RPCs to other servers
    def crash(self):
        """Crashes this metadata store"""
        print("Crash()")
        '''When crash is called on a leader node it should be restored as a follower, the term and fileinfomap should persist. 
        Also it should still respond the isCrashed(), isLeader()tr, tester_getversion() rpc calls while raising an error on the rest. '''
        self.crashState = True
        if self.state == self.LEADER:
            self.state = self.FOLLOWER
        # raise xmlrpc.client.Fault
        return True

    # "Restores" this metadata store, allowing it to start responding
    # to and sending RPCs to other nodes
    def restore(self):
        """Restores this metadata store"""
        print("Restore()")
        self.crashState = False
        self.state = self.FOLLOWER
        return True

    # "IsCrashed" returns the status of this metadata node (crashed or not)
    # This method should always work, even when the node is crashed
    def isCrashed(self):
        """Returns whether this node is crashed or not"""
        print("IsCrashed()")
        # if self.crashState:
        #     raise xmlrpc.client.Fault
        return self.crashState

    # Requests vote from this server to become the leader
    def requestVote(self, serverid, term):
        """Requests vote to be the leader"""
        try:
            # if self is follower and needs to request vote, use stored connections to call peers
            # print('sending vote request')

            new_term, success = self.serverlist[serverid].surfstore.requestVoteRPC(term, self.id)
            # print('receive vote')
            if new_term != -1:
                with self.controller_var_lock:
                    self.lifeCount += 1
            if new_term == self.currentTerm and success:
                with self.controller_var_lock:
                    self.vote += 1
            elif new_term > self.currentTerm:
                with self.controller_var_lock:
                    self.currentTerm = new_term
                    self.vote = 0
                    self.state = self.FOLLOWER
                    self.elec_timer_messages.put(self.RESET_ELEC_TIMEOUT)
        except Exception as e:
            # if e.errno == errno.ECONNREFUSED:
            #     print('?????????????????????')
            print(e, 'vote')
            pass

    def requestVoteRPC(self,term, candidateId):

        with self.lock:
            try:
                if self.crashState:
                    return -1,False
                if term < self.currentTerm:
                    return self.currentTerm, False
                elif term == self.currentTerm:

                    if self.state != self.LEADER and \
                            (self.votedFor is None or self.votedFor == candidateId):
                        self.currentTerm = term
                        self.votedFor = candidateId
                        self.state = self.FOLLOWER
                        self.last_update = time.time()
                        return self.currentTerm, True
                    return self.currentTerm, False
                else:
                    self.currentTerm = term
                    self.votedFor = candidateId
                    self.state = self.FOLLOWER
                    self.last_update = time.time()
                    return self.currentTerm, True

            except Exception as e:
                print(e,'vote rpc')
                pass

    # Updates fileinfomap
    def appendEntries(self, serverid, term, fileinfomap):
        """Updates fileinfomap to match that of the leader"""
        try:

            term, success = self.serverlist[serverid].surfstore.appendEntriesRPC(term, self.id, fileinfomap)
            # print('serverid ',serverid)
            if term != -1:
                with self.controller_var_lock:
                    self.lifeCount += 1
            if success:
                self.commitIndex += 1
            elif term > self.currentTerm:
                with self.controller_var_lock:
                    self.state = self.FOLLOWER
                    self.elec_timer_messages.put(self.RESET_ELEC_TIMEOUT)
            else:
                self.nextIndex -= 1

        except Exception as e:
            print(e,'append')
            pass

    def appendEntriesRPC(self,term, leaderId, fileinfomap):
        # global state, currentTerm, votedFor, lock
        with self.lock:
            try:
                if self.crashState:
                    return False, -1

                if term < self.currentTerm:
                    return False, self.currentTerm
                else:
                    self.currentTerm = term
                    self.votedFor = None
                    self.fileinfomap = fileinfomap
                    self.last_update = time.time()

                    return True, self.currentTerm
            except Exception as e:
                print(e,'append rpc')
                pass

    def tester_getversion(self,filename):
        try:
            if filename in self.fileinfomap.keys():
                return self.fileinfomap[filename]['version']
            else:
                return None
        except Exception as e:
            print('no key')
            pass
    def controller(self):
        print('controller()')

        # self.last_update = time.time()
        self.controller_var_lock = threading.Lock()
        self.electionTimeout = 1.5 * random.random() + 1
        while True:
            try:

                m = self.controller_messages.get(timeout=self.electionTimeout)
                if m == self.APPEND_ENTRY_RECEIVED:
                    self.elec_timer_messages.put(self.RESET_ELEC_TIMEOUT)
                    self.state = self.FOLLOWER
                elif self.state != self.LEADER and not self.crashState and m == self.ELECTION:
                    self.controller_var_lock.acquire()
                    self.vote = 1
                    self.lifeCount = 1
                    self.state = self.CANDIDATE
                    self.currentTerm += 1
                    self.votedFor = self.id
                    self.controller_var_lock.release()
                    threads = {}
                    for peerId, peer in self.serverlist.items():
                        if peerId == id:
                            continue
                        threads[peerId] = threading.Thread(target=self.requestVote, args=(peerId, self.currentTerm))
                        threads[peerId].start()
                        # self.requestVote(peerId, self.currentTerm)
                    for i in threads.keys():
                        threads[i].join(timeout=self.electionTimeout)

                    # print(id,' vote:', vote)
                    if (self.state == self.CANDIDATE) and (self.vote >= int(0.5 * (len(self.serverlist) + 1))+1):
                        # print('majority alive? ',self.lifeCount >= 0.5 * (len(self.serverlist) + 1))
                        if (self.lifeCount >= int(0.5 * (len(self.serverlist) + 1))+1):
                            self.state = self.LEADER
                            print('New Leader:     ', self.id, ' life servers: ', self.lifeCount)
                            self.elec_timer_messages.put(self.RESET_ELEC_TIMEOUT)
                            self.controller_messages.put(self.SEND_HEART_BEAT)

                    elif (self.state == self.CANDIDATE) and (self.vote < int(0.5 * (len(self.serverlist) + 1))+1):
                        self.controller_var_lock.acquire()
                        self.votedFor = None
                        self.controller_var_lock.release()
                        self.elec_timer_messages.put(self.ELEC_FAIL_RELECT)

                # print(self.state, self.vote,self.state == self.LEADER and m == self.SEND_HEART_BEAT, self.id)
                if self.state == self.LEADER and m == self.SEND_HEART_BEAT:
                    print('controller send heart beat. Current term:', self.id, self.currentTerm)
                    threads = {}
                    with self.controller_var_lock:
                        self.lifeCount = 1
                    for peerId, peer in self.serverlist.items():
                        if peerId == id:
                            continue
                        threads[peerId] = threading.Thread(target=self.appendEntries,
                                                           args=(peerId, self.currentTerm, self.fileinfomap))
                        threads[peerId].start()
                        # self.requestVote(peerId, self.currentTerm)
                    for i in threads.keys():
                        threads[i].join(timeout=self.heartbeatTimeout)
                    # print('living peer:', self.lifeCount)

            except Exception as e:
                print(e,'controller')
                pass
        pass

    def heartbeat(self):
        print('heartbeat()')
        # print('heartbeat', state)
        try:
            for peerId, peer in self.serverlist.items():
                if peerId == id:
                    continue
                term, success = self.appendEntries(peer, self.currentTerm, self.fileinfomap)
                if success:
                    pass
                elif term > self.currentTerm:
                    with self.controller_var_lock:
                        self.currentTerm = term
                        self.state = FOLLOWER
                        self.elec_timer_messages.put(self.RESET_ELEC_TIMEOUT)
        except Exception as e:
            pass

    def election_timer(self):
        # print('election timer()')
        self.electionTimeout = 1.5 * random.random() + 1
        while True:
            try:
                # print('get timer msg')
                m = self.elec_timer_messages.get(timeout=self.electionTimeout)
                if m == self.RESET_ELEC_TIMEOUT:
                    self.electionTimeout = 1.5 * random.random() + 1
                    self.last_update = time.time()
                if m == self.ELEC_FAIL_RELECT:
                    pass
            except Exception as e:
                # print(e, 'election err')
                pass
            if time.time() > self.last_update + self.electionTimeout and not self.crashState:
                # print('timeout for election')
                self.controller_messages.put(self.ELECTION)
                self.electionTimeout = 1.5 * random.random() + 1
                self.last_update = time.time()

    def heart_beat_timer(self):
        hb_interval = 0.2
        while True:
            self.controller_messages.put(self.SEND_HEART_BEAT)
            time.sleep(hb_interval)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')
        # parser.add_argument('port', type=int, help='port')
        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers

        print("Attempting to start XML-RPC Server...")
        surfserver = Surfstore(config,servernum)

    except Exception as e:
        print("Server: " + str(e))

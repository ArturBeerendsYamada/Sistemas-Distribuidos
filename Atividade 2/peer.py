import Pyro5.api
import sys
import threading
import time
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

RELEASED = 0
WANTED = 1
HELD = 2
ACCEPTED = 3
DENIED = 4
SKIPPED = 5
HEARTBEAT_INTERVAL = 5 # seconds between heartbeats
HEARTBEAT_TOLERANCE = 2 # number of missed heartbeats until a peer is considered inactive
REQUEST_TIMEOUT = 20 # maximum time one waits for responses
CRITSEC_TIMEOUT = 20 # maximum time one can hold the critical section

Pyro5.config.PREFER_IP_VERSION = 4

def get_peer_uri(peer, debug=True):
    nameserver = Pyro5.api.locate_ns(host="127.0.0.1")
    # try:
    uri = nameserver.lookup(peer)
    # except Exception as e:
    #     if debug:
    #         print(f"Error looking up {peer}: {e}")
    #     return None
    peerProxy = Pyro5.api.Proxy(uri)
    return peerProxy

class pyroPeer(object):
    name = "peerX" 
    state = RELEASED
    request_timestamp = 0
    cs_timeout_scheduler = BackgroundScheduler()
    peerList = ["peerA", "peerB", "peerC", "peerD"]
    active_peers = {
        "peerA": {"active": False, "heartbeat": 20, "last_reply": SKIPPED},
        "peerB": {"active": False, "heartbeat": 20, "last_reply": SKIPPED},
        "peerC": {"active": False, "heartbeat": 20, "last_reply": SKIPPED},
        "peerD": {"active": False, "heartbeat": 20, "last_reply": SKIPPED},
    }
    queued_requests = []
    
    def __init__(self, name):
        self.name = name
        self.check_active_peers()
        self.cs_timeout_scheduler.start()
        print("Peer {0} initialized".format(self.name))
    
    def receiveHeartbeat(self, peer):
        self.active_peers[peer]["heartbeat"] = 0
        self.active_peers[peer]["active"] = True

    # sends own heartbeat for other peers and checks if any are inactive
    def check_heartbeats(self):
        # sends heartbeats to others
        for peer in self.peerList:
            if not peer == self.name:
                # print("Heartbeating {0}".format(peer))
                # registered_name = "PYRONAME:"+peer
                # peerProxy = Pyro5.api.Proxy(registered_name)
                try:
                    peerProxy = get_peer_uri(peer, debug=False)
                    peerProxy.heartbeat(self.name)
                except:
                    pass
        
        # updates others status based on previously heartbeats
        for peer in self.peerList:
            if not peer == self.name:
                self.active_peers[peer]["heartbeat"] += HEARTBEAT_INTERVAL
                if self.active_peers[peer]["heartbeat"] > HEARTBEAT_TOLERANCE * HEARTBEAT_INTERVAL:
                    self.active_peers[peer]["active"] = False
                # print(peer, self.active_peers[peer]["active"], self.active_peers[peer]["heartbeat"])
    
    def check_active_peers(self, detailed=True):
        # print(Pyro5.api.locate_ns(host="127.0.0.1").list())
        for peer in self.peerList:
            if not peer == self.name:
                # registered_name = "PYRONAME:"+peer
                # peerProxy = Pyro5.api.Proxy(registered_name)
                try:
                    peerProxy = get_peer_uri(peer)
                    peer_reply = peerProxy.identify()
                    if detailed:
                        print(peer_reply)
                    self.active_peers[peer]["active"] = True
                    self.active_peers[peer]["heartbeat"] = 0
                except Exception as e:
                    self.active_peers[peer]["active"] = False
                    if detailed:
                        print(f"{peer} unreachable due to error: [{e}]")
    
    def enter_critical_section(self):
        self.state = WANTED
        if self.request_timestamp == 0:
            self.request_timestamp = time.time()

        while self.state == WANTED:
            # checks active peers before requesting
            self.check_active_peers(detailed=False)
            # sends requests to all active peers that haven't replied yet
            for peer in self.peerList:
                if not peer == self.name and self.active_peers[peer]["active"] and self.active_peers[peer]["last_reply"] == SKIPPED:
                    # print("Requesting CS from {0}".format(peer))
                    # registered_name = "PYRONAME:"+peer
                    # peerProxy = Pyro5.api.Proxy(registered_name)
                    try:
                        peerProxy = get_peer_uri(peer)
                        reply = peerProxy.request_cs(self.name, self.request_timestamp)
                        # print("Reply from {0}: {1}".format(peer, reply))
                        self.active_peers[peer]["last_reply"] = reply
                    except Exception as e:
                        print(f"Error requesting CS from {peer}: {e}")
                        self.active_peers[peer]["active"] = False
                elif self.active_peers[peer]["active"] == False:
                    self.active_peers[peer]["last_reply"] = SKIPPED
            # check if all replies are ACCEPTED, if one is denied, go back to WANTED and retry (they should reply when free)
            if (time.time() - self.request_timestamp) > REQUEST_TIMEOUT:
                print("Request timeout exceeded, aborting CS request")
                self.state = RELEASED
                self.leave_critical_section()
                return
            print("Attempting to enter critical section...")
            self.state = HELD
            for peer in self.peerList:
                if not peer == self.name and self.active_peers[peer]["active"]:
                    if self.active_peers[peer]["last_reply"] == DENIED:
                        self.state = WANTED
                        print("CS request DENIED by {0}".format(peer))
            time.sleep(1)

        # if successfully entered critical section, schedule timeout to release it
        if self.state == HELD:
            print("Entering critical section")
            # schedule a timeout to release the critical section if held too long
            datetime_now = datetime.datetime.now()
            datetime_timeout = datetime_now + datetime.timedelta(seconds=CRITSEC_TIMEOUT)
            self.cs_timeout_scheduler.add_job(release_critical_section_by_timeout,
                                              "date", run_date=datetime_timeout, id="cs_timeout")
    
    def leave_critical_section(self):
        print("Leaving critical section")
        # release clean up
        self.state = RELEASED
        self.request_timestamp = 0
        for peer in self.peerList:
            if not peer == self.name:
                self.active_peers[peer]["last_reply"] = SKIPPED
        self.cs_timeout_scheduler.remove_all_jobs()

        # notify all queued requests that critical section is released
        for peer in self.queued_requests:
            print("Notifying {0} of CS release".format(peer))
            # registered_name = "PYRONAME:"+peer
            # peerProxy = Pyro5.api.Proxy(registered_name)
            try:
                peerProxy = get_peer_uri(peer)
                peerProxy.delayed_cs_accept(self.name)
            except Exception as e:
                print(f"Error notifying {peer} of CS release: {e}")
                self.active_peers[peer]["active"] = False
        self.queued_requests = []


@Pyro5.api.expose
class pyroExposedPeer(object):

    @Pyro5.api.oneway
    def heartbeat(self, peer):
        myPeer.receiveHeartbeat(peer)

    def identify(self):
        return "{0} active".format(myPeer.name)

    def request_cs(self, peer, timestamp):
        # if self state is HELD, queue request and return DENIED
        if myPeer.state == HELD:
            if peer not in myPeer.queued_requests:
                myPeer.queued_requests.append(peer)
            return DENIED
        # if self state is WANTED, compare timestamps and peer names, priority to the lower
        elif myPeer.state == WANTED:
            if timestamp > myPeer.request_timestamp:
                if peer not in myPeer.queued_requests:
                    myPeer.queued_requests.append(peer)
                return DENIED
            elif timestamp == myPeer.request_timestamp:
                if peer > myPeer.name:
                    if peer not in myPeer.queued_requests:
                        myPeer.queued_requests.append(peer)
                    return DENIED
                else:
                    return ACCEPTED
            else:
                return ACCEPTED
        # if self state is else, hopefully RELEASED, return ACCEPTED
        else:
            return ACCEPTED

    @Pyro5.api.oneway
    def delayed_cs_accept(self, peer):
        myPeer.active_peers[peer]["last_reply"] = ACCEPTED


def run_daemon(name):
    # make a Pyro daemon, available global for shutdown when program exits
    global daemon
    daemon = Pyro5.server.Daemon(host="127.0.0.1")
    try:
        # find the name server
        ns = Pyro5.api.locate_ns(host="127.0.0.1")
    except:
        print("\nNo Name Server located")
        # threading.Thread(target=Pyro5.nameserver.start_ns_loop).start()
        # ns = Pyro5.api.locate_ns()
    uri = daemon.register(pyroExposedPeer)            # register the object as a Pyro object
    ns.register(name, uri)                      # register the object with a name in the name server
    daemon.requestLoop()                        # start the event loop of the server to wait for calls


def tick():
    # print("---------- Heartbeat ----------")
    myPeer.check_heartbeats()
    pass

def release_critical_section_by_timeout():
    if myPeer.state == HELD:
        print("Releasing critical section due to timeout")
        myPeer.leave_critical_section()

# Check if a string argument is provided to name the peer
if len(sys.argv) > 1:
    arg = sys.argv[1]
    name = "peer" + arg
else:
    print("No argument provided!")
    sys.exit()

# create pyroPeer object
global myPeer
myPeer = pyroPeer(name)

# start the Pyro daemon on another thread
thread = threading.Thread(target=run_daemon, args=(name, ))
thread.start()


# start the scheduler for checking heartbeats
scheduler = BackgroundScheduler()
scheduler.add_job(tick, "interval", seconds=HEARTBEAT_INTERVAL)
scheduler.start()

try:
    # user interface loop
    while 1:
        command = input("Commands:\n1. Request\n2. Release\n3. List\n")
        if command == '1':
            if myPeer.state == HELD:
                print("Already in critical section")
                continue
            myPeer.enter_critical_section()
        elif command == '2':
            if myPeer.state != HELD:
                print("Not in critical section")
                continue
            myPeer.leave_critical_section()
        elif command == '3':
            myPeer.check_active_peers()
            
        else:
            print("Unknown command")
        
        
except:
    print("Cleaning up and exiting")
    # clean up and exit
    ns = Pyro5.api.locate_ns(host="127.0.0.1")
    ns.remove(name)
    daemon.shutdown()
    scheduler.shutdown()
    print("Exited cleanly")
    sys.exit()



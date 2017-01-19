/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <random>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */

    this->memberNode->inGroup = false;
    this->memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

    vector<MemberListEntry> memberList;
    MessageHdr *msgHdr = (MessageHdr *)(data);


    if (msgHdr->msgType == JOINREQ) {

        int *id = (int *) (data + sizeof(MessageHdr));
        short *port = (short *)(data + sizeof(MessageHdr) + sizeof(int));
        long *heartbeat = (long *)(data + sizeof(MessageHdr) + sizeof(int) + sizeof(short) + 1);

        Address a = getAddress(*id, *port);

        addOrUpdate(&a, *heartbeat);
        int msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        msg->msgType = JOINREP;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        emulNet->ENsend(&memberNode->addr, &a, (char *)msg, msgsize);
        free(msg);
    }

    if (msgHdr->msgType == JOINREP) {
        memberNode->inGroup = true;
    }

    if (msgHdr->msgType == HEARTBEAT) {
        int num = (size - sizeof(MessageHdr)) / (sizeof(Address) + sizeof(long));
        char * pt;
        pt = data + sizeof(MessageHdr);
        for (int i = 0; i < num; i++) {
            int *id = (int *) (pt);
            short *port = (short *)(pt + sizeof(int));
            long *heartbeat = (long *)(pt + sizeof(int) + sizeof(short));
            Address a = getAddress(*id, *port);
            if (par->getcurrtime() == 10 ) {
                printf("");
            }
            addOrUpdate(&a, *heartbeat);
            pt = pt + sizeof(int) + sizeof(short) + sizeof(long);
        }
    }


}

bool MP1Node::addOrUpdate(Address *addr, long heartbeat) {
    if (!hasMember(addr, heartbeat)) {
        MemberListEntry member = MemberListEntry(getId(addr), getPort(addr), heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(member);
        log->logNodeAdd(&memberNode->addr, addr);
    }
}

bool MP1Node::hasMember(Address *joinaddr, long heartbeat) {
    for (int i=0; i < memberNode->memberList.size(); i++) {
        if (memberNode->memberList[i].getid() == getId(joinaddr) &&
                memberNode->memberList[i].getport() == getPort(joinaddr)) {
            if (memberNode->memberList[i].heartbeat < heartbeat) {
                memberNode->memberList[i].setheartbeat(heartbeat);
                memberNode->memberList[i].settimestamp(par->getcurrtime());
            }
            return true;
        }
    }
    return false;
}

void MP1Node::printMemberListTable() {
    int i = 0;
    printf("Printing MemberListTable: %d elements. I am: ",memberNode->memberList.size()   );
    printAddress(&memberNode->addr);
    for ( i = 0; i <  memberNode->memberList.size() ; i++){
        MemberListEntry curr = memberNode->memberList.at(i);
        printf("\t%d: Address: %d:%x, Hb: %lu, Timetamp: %lu\n",
               i, curr.getid(), curr.getport() , curr.getheartbeat(), curr.gettimestamp() );


    }

}

int MP1Node::getId( Address * idadress) {
    int id = 0;
    memcpy(&id, &idadress->addr[0], sizeof(int));
    return id;
}

short MP1Node::getPort(Address * address) {
    short port = 0;
    memcpy(&port, &address->addr[4], sizeof(short));
    return port;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat = par->getcurrtime();

    vector<MemberListEntry> *l = checkMembership();

    if (l->size() != 0) gossipMembership(l);

    return;
}

void MP1Node::gossipMembership(vector<MemberListEntry> *l) {
    size_t size = ((l->size() + 1) * (sizeof(int) +  sizeof(short) + sizeof(long)) + sizeof(MessageHdr))* sizeof(char);
    MessageHdr *msg;
    msg = (MessageHdr *) malloc(size);
    char *pt = (char *) msg;
    msg->msgType = HEARTBEAT;
    pt = pt + sizeof(MessageHdr);


    if (par->getcurrtime() == 9 && getId(&memberNode->addr) == 1) {
        printf("");
    }

    memcpy(pt, &memberNode->addr, sizeof(Address));
    memcpy(pt + sizeof(short) + sizeof(int), &memberNode->heartbeat, sizeof(long));
    pt = pt + sizeof(short) + sizeof(int) + sizeof(long);

    vector<MemberListEntry> list = *l;
    vector<MemberListEntry>::iterator iter = list.begin();
    unsigned seed = 0;
    std::shuffle(l->begin(), l->end(), std::default_random_engine(seed));
    int count = list.size();
    while(iter != list.end()) {
        if (iter->heartbeat >= 0) {
            long hear = iter->heartbeat;
            Address a = getAddress(iter->id, iter->port);
            memcpy(pt, &a, sizeof(Address));
            memcpy(pt + sizeof(Address), &hear, sizeof(long));
            pt = pt + sizeof(Address) + sizeof(long);
        }
        iter++;
    }
    iter = l->begin();
    while(iter != list.end()) {
        if (iter->heartbeat >= 0) {
            Address a = getAddress(iter->id, iter->port);
            if (count > 0) {
                emulNet->ENsend(&memberNode->addr, &a, (char *)msg, size);
                count--;
            } else {
                break;
            }
        }
        iter++;
    }

    free(msg);
}

vector<MemberListEntry> * MP1Node::checkMembership() {
    vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
    vector<MemberListEntry> *l = new vector<MemberListEntry>();
    int count = 0;
    while(iter != memberNode->memberList.end()){
        int id = iter->id;
        short port = iter->port;
        long heartbeat = iter->heartbeat;
        long timestamp = iter->timestamp;
        long diff = par->getcurrtime() - timestamp;
        Address currAddr = getAddress(id,port);

        if (diff > TREMOVE) {
            memberNode->memberList.erase(iter);
            log->logNodeRemove(&memberNode->addr, &currAddr);
        } else {
            if (diff < TFAIL) {
                l->push_back(*iter);
            }
            iter++;
        }
    }
    return l;
}

Address MP1Node::getAddress(int address, short port) {

    Address addr;
    memset(&addr, 0, sizeof(Address));
    *(int *)(&addr.addr) = address;
    *(short *)(&addr.addr[4]) = port;

    return addr;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    // Address addr = getJoinAddress();
    // MemberListEntry m = MemberListEntry(getId(&addr), getPort(&addr), 0, par->getcurrtime());
    // memberNode->memberList.push_back(m);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

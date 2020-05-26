# coding: utf-8
import sys
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplList, ReplDict
from argparse import ArgumentParser
import zerorpc
from raft_server import get_node_info


class DistributedList(object):

    def __init__(self, args):
        self.msg_list = ReplList()
        cfg = SyncObjConf(dynamicMembershipChange=True, bindAddress='0.0.0.0:' + args.address.split(':')[-1])
        self._control_port = ReplDict()
        SyncObj(args.address, args.partner, consumers=[self.msg_list, self._control_port], conf=cfg)
        self.args = args
        self._control_port.set(args.address, args.control_port)

    def append(self, msg):
        self.msg_list.append(msg, sync=True)

    def get_data(self):
        return self.msg_list.rawData()

    def get_node_info(self):
        info = get_node_info(self.args.address)
        nodes = [node for node in info['partner_nodes']]
        nodes.append(info['self'])
        nodes = [(node.split(':')[0] + ':' + self._control_port[node]) for node in nodes]
        leader_node = info['leader'].split(':')[0] + ':' + self._control_port[info['leader']]
        return {
            'leader_node': leader_node,
            'nodes': nodes,
            'detail': info
        }


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-address')
    parser.add_argument('-partner', nargs='*')
    parser.add_argument('-control_port')
    args = parser.parse_args(sys.argv[1:])
    dis_list = DistributedList(args)
    s = zerorpc.Server(dis_list)
    control_add = 'tcp://0.0.0.0:' + args.control_port
    s.bind(control_add)
    print('control address: ' + control_add)
    s.run()

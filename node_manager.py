# coding: utf-8
# @Time     : 2020-05-24 18:50
# @Author   : zengfangrui
# @Email    : 978927654@qq.com
# @File     : node_manager
# @Software : PyCharm
import zerorpc
import time


class NodeManager(object):

    def __init__(self, address, error_expire=2 * 60):
        self.error_nodes = dict()  # 崩掉的节点
        self.error_expire = error_expire
        self.node_user_cnt = None
        if not self.init_helper(address):
            raise Exception('can not connect to raft server')

    def init_helper(self, address):
        # print('init node manager with address : %s' % address)
        init_client = zerorpc.Client(timeout=3)
        try:
            init_client.connect('tcp://' + address)
            node_info = init_client.get_node_info()
        except (zerorpc.exceptions.TimeoutExpired, zerorpc.exceptions.LostRemote, zerorpc.exceptions.RemoteError):
            print('init raft server address error')
            return False
        self.leader_node = node_info['leader_node']
        self.nodes = node_info['nodes']
        if not self.node_user_cnt:
            self.node_user_cnt = dict()  # 每个节点用户数，决定下一个请求使用的节点
            for node in self.nodes:
                self.node_user_cnt[node] = 0
        self._init_clients()
        return True

    def _init_clients(self):
        self.clients = dict()
        for node in self.nodes:
            client = zerorpc.Client(timeout=3)
            client.connect('tcp://' + node)
            self.clients[node] = client

    def choose_node(self):
        try:
            self.node_user_cnt = {k: v for k, v in sorted(self.node_user_cnt.items(), key=lambda item: item[1])}
            chosen_node = list(self.node_user_cnt.keys())
            while self.in_error_node(chosen_node[0]):
                chosen_node = chosen_node[1:]
            chosen_node = chosen_node[0]
            self.node_user_cnt[chosen_node] += 1
        except IndexError:  # no available node
            return None
        return chosen_node

    def catch_error_node(self, error_node):
        self.error_nodes[error_node] = time.time()
        for node in self.nodes:
            if node == error_node:
                continue
            if self.init_helper(node):
                return True
        return False

    def get_leader(self):
        return self.leader_node

    def get_client_by_node(self, node):
        return self.clients[node]

    def in_error_node(self, node):
        for error_node, start_time in list(self.error_nodes.items()):
            if time.time() - start_time > self.error_expire:
                self.error_nodes.pop(error_node)
                continue
            if node == error_node:
                return True
        return False


if __name__ == '__main__':
    manager = NodeManager('127.0.0.1:8000')
    # print(manager.clients, manager.error_nodes)
    # print(manager.node_user_cnt)
    # print(manager.choose_node(), manager.choose_node(), manager.choose_node(), manager.choose_node())
    # import time
    #
    # time.sleep(6)
    # manager.catch_error_node('127.0.0.1:8000')
    # print(manager.clients, manager.error_nodes)
    # print(manager.choose_node(), manager.choose_node(), manager.choose_node(), manager.choose_node())

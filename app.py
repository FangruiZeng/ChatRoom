# coding: utf-8
import sys
import asyncio
from functools import partial
import time
import zerorpc
from argparse import ArgumentParser
from pywebio import start_server
from pywebio.input import *
from pywebio.output import *
from pywebio.session import *
from node_manager import NodeManager

# 管理员账户名
ADMIN_USER = '📢'

user_names = set()

node_manager = None


def catch_error(func):
    """Catch errors of node disconnect"""

    def wrap(self, *args, **kwargs):
        while True:
            try:
                return func(self, *args, **kwargs)
            except (zerorpc.exceptions.TimeoutExpired, zerorpc.exceptions.LostRemote, zerorpc.exceptions.RemoteError):
                self.send_msg('节点 %s 无法连接，重定向至可用节点中...' % self.node_name, is_admin=True, myself=True)
                # print('节点 %s 无法连接，重定向至可用节点中...' % self.node_name)
                if not node_manager.catch_error_node(error_node=self.node_name):
                    self.send_msg("所有节点均不可用，请稍后再试", is_admin=True, myself=True)
                    return False
                    # raise Exception("all raft nodes don't work")
                node_manager.node_user_cnt[self.node_name] -= 1
                new_node = node_manager.choose_node()
                if not self.node_name:
                    self.send_msg("所有节点均不可用，请稍后再试", is_admin=True, myself=True)
                    return False
                # print('new node: %s' % new_node)
                self.read_client = node_manager.get_client_by_node(new_node)
                self.write_client = node_manager.get_client_by_node(node_manager.get_leader())
                # print('leader node %s' % node_manager.get_leader())
                self.send_msg('节点 %s 无法连接，已重定向至节点 %s' % (self.node_name, new_node), is_admin=True, myself=True)
                self.node_name = new_node
    return wrap


class MsgManager(object):
    global node_manager

    def __init__(self, nickname):
        self.nickname = nickname
        user_names.add(nickname)
        self.node_name = node_manager.choose_node()  # 选择负载最低的节点
        if self.node_name:
            self.read_client = node_manager.get_client_by_node(self.node_name)  # 读操作通过负载低的节点进行
            # 写操作通过leader进行，因为raft算法其他节点也只是转发写请求到leader，直接leader少一次请求转发
            self.write_client = node_manager.get_client_by_node(node_manager.get_leader())

    def send_msg(self, content, is_admin=False, instant_output=True, myself=False):
        """向聊天室发送消息

        :param bool is_admin: 消息发送者是否是系统
        :param str content: 消息内容，markdown格式字符串
        :param bool instant_output: 是否立即向当前会话输出此消息
        :param bool myself: 是否仅向当前会话输出此消息
        """
        if is_admin:
            user = ADMIN_USER
        else:
            user = '%s@%s' % (self.nickname, self.node_name)
        if not myself:
            if not self._send_chat_msg(user, content):
                return
        if instant_output or myself:
            put_markdown('`%s`: %s' % (user, content))

    async def refresh_msg(self):
        """刷新聊天消息

        将全局聊天记录列表中新增的聊天记录发送到当前会话，但排除掉当前用户的消息，当前用户的消息会在用户提交后直接输出
        """
        chat_msgs = self._get_chat_msgs()
        last_idx = len(chat_msgs)
        while True:
            await asyncio.sleep(0.5)
            chat_msgs = self._get_chat_msgs()
            if chat_msgs is False:  # 没能获取到聊天记录，说明没有可用节点了，退出刷新聊天记录
                break
            for m in chat_msgs[last_idx:]:
                if m[0] != '%s@%s' % (self.nickname, self.node_name):  # 仅刷新其他人的新信息
                    put_markdown('`%s`: %s' % (m[0], m[1]))

            last_idx = len(chat_msgs)
        put_text("你已经退出聊天室")

    @catch_error
    def _get_chat_msgs(self):
        return self.read_client.get_data()

    @catch_error
    def _send_chat_msg(self, user, content):
        self.write_client.append((user, content))
        return True


async def main():
    global node_manager

    set_output_fixed_height(True)
    set_title("Chat Room")
    put_markdown("""欢迎来到聊天室，聊天内容将存储在Raft集群的多个节点上\n""", lstrip=True)

    nickname = await input("请输入你的昵称", required=True,
                           valid_func=lambda n: '昵称已被使用' if n in user_names or n == ADMIN_USER else None)

    msg_manager = MsgManager(nickname)

    if msg_manager.node_name:
        msg = '`%s`加入聊天室. 所在节点为 %s, 所在节点在线人数 %s, 全节点在线人数 %s' % (
            nickname, msg_manager.node_name, node_manager.node_user_cnt[msg_manager.node_name],
            sum(node_manager.node_user_cnt.values()))
        msg_manager.send_msg(msg, is_admin=True)
    else:
        msg_manager.send_msg("所有节点均不可用，请稍后再试", is_admin=True, myself=True)

    @defer_call
    def on_close():
        node_manager.node_user_cnt[msg_manager.node_name] -= 1
        msg_manager.send_msg('`%s`退出聊天室. 所在节点在线人数 %s, 全节点在线人数 %s' % (
            nickname, node_manager.node_user_cnt[msg_manager.node_name], sum(node_manager.node_user_cnt.values())),
                              is_admin=True,
                              instant_output=False)
        user_names.remove(nickname)

    # 启动后台任务来刷新聊天消息
    time.sleep(0.5)
    refresh_task = run_async(msg_manager.refresh_msg())

    while True:
        data = await input_group('发送消息', [
            input(name='msg', help_text='消息内容支持Markdown 语法', required=True),
            actions(name='cmd', buttons=['发送', {'label': '退出', 'type': 'cancel'}])
        ])
        if data is None:
            break

        msg_manager.send_msg(data['msg'])

    # 关闭后台任务
    refresh_task.close()

    put_text("你已经退出聊天室")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-raft_addr')
    args = parser.parse_args(sys.argv[1:])
    node_manager = NodeManager(args.raft_addr)
    start_server(partial(main), debug=False, auto_open_webbrowser=True)

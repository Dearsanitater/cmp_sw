import json
from channels.generic.websocket import AsyncWebsocketConsumer

class MessageConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # 不使用用户 ID，直接连接到一个默认的消息组
        self.room_group_name = "messages_group"  # 所有 WebSocket 共享的消息组

        # 加入消息组
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )

        # 同意 WebSocket 连接
        await self.accept()

    async def disconnect(self, close_code):
        # 离开消息组
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )

    # 从消息组接收到消息并转发给 WebSocket
    async def send_message(self, event):
        message = event['message']
        rule_id = event['rule_id']  # 假设消息中带有规则 ID

        # 发送消息到 WebSocket
        await self.send(text_data=json.dumps({
            'message': message,
            'rule_id': rule_id  # 将规则 ID 一起发送到前端
        }))

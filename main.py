from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
import aiomysql
import json
from typing import Optional


@register("mysql_logger", "LW", "MySQL消息日志插件", "1.0.0")
class MySQLPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None  # 先初始化为 None

    async def initialize(self):
        """初始化MySQL连接池"""
        try:
            # 正确创建连接池
            self.pool = await aiomysql.create_pool(
                host=self.config.get("host"),
                port=self.config.get("port"),
                user=self.config.get("username"),
                password=self.config.get("password"),
                db=self.config.get("database"),
                autocommit=True,
                minsize=1,
                maxsize=5
            )

            # 测试连接
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    result = await cursor.fetchone()
                    if result[0] != 1:
                        raise ConnectionError("数据库测试查询失败")

            # 创建表结构
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                                         CREATE TABLE IF NOT EXISTS messages
                                         (
                                             message_id    VARCHAR(255) PRIMARY KEY,
                                             platform_type VARCHAR(50)  NOT NULL,
                                             self_id       VARCHAR(255) NOT NULL,
                                             session_id    VARCHAR(255) NOT NULL,
                                             group_id      VARCHAR(255),
                                             sender        JSON         NOT NULL,
                                             message_str   TEXT         NOT NULL,
                                             raw_message   LONGTEXT,
                                             timestamp     INT          NOT NULL
                                         )
                                         """)
        except Exception as e:
            # 建议添加更详细的错误处理
            print(f"初始化失败: {str(e)}")
            raise

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        """处理所有消息事件"""
        try:
            # 从事件对象中获取消息信息
            msg = event.message_obj  # 消息对象
            meta = event.platform_meta  # 平台元数据

            # 序列化发送者信息（根据实际MessageMember结构调整）
            sender_data = {
                'user_id': msg.sender.user_id,
                'nickname': msg.sender.nickname,
                'platform_id': meta.id
            }

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                                         INSERT INTO messages (message_id,
                                                               platform_type,
                                                               self_id,
                                                               session_id,
                                                               group_id,
                                                               sender,
                                                               message_str,
                                                               raw_message,
                                                               timestamp)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         """, (
                                             msg.message_id,
                                             meta.name,  # 平台类型枚举值
                                             event.get_self_id(),
                                             event.session_id,
                                             msg.group_id or None,
                                             json.dumps(sender_data),
                                             event.message_str,  # 使用事件中的消息字符串
                                             json.dumps(msg.raw_message),  # 原始消息对象
                                             msg.timestamp
                                         ))
        except Exception as e:
            raise

    async def terminate(self):
        """清理资源"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

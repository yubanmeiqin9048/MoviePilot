import json
import threading
from typing import List, Union, Generator

from sqlalchemy.orm import Session

from app import schemas
from app.chain import ChainBase
from app.core.config import settings
from app.db import SessionFactory
from app.db.mediaserver_oper import MediaServerOper
from app.log import logger
from app.schemas import MessageChannel, Notification

lock = threading.Lock()


class MediaServerChain(ChainBase):
    """
    媒体服务器处理链
    """

    def __init__(self, db: Session = None):
        super().__init__(db)

    def librarys(self) -> List[schemas.MediaServerLibrary]:
        """
        获取媒体服务器所有媒体库
        """
        return self.run_module("mediaserver_librarys")

    def items(self, library_id: Union[str, int]) -> Generator:
        """
        获取媒体服务器所有项目
        """
        return self.run_module("mediaserver_items", library_id=library_id)

    def episodes(self, item_id: Union[str, int]) -> List[schemas.MediaServerSeasonInfo]:
        """
        获取媒体服务器剧集信息
        """
        return self.run_module("mediaserver_tv_episodes", item_id=item_id)

    def remote_sync(self, channel: MessageChannel, userid: Union[int, str]):
        """
        同步豆瓣想看数据，发送消息
        """
        self.post_message(Notification(channel=channel,
                                       title="开始媒体服务器 ...", userid=userid))
        self.sync()
        self.post_message(Notification(channel=channel,
                                       title="同步媒体服务器完成！", userid=userid))

    def sync(self):
        """
        同步媒体库所有数据到本地数据库
        """
        with lock:
            # 媒体服务器同步使用独立的会话
            _db = SessionFactory()
            _dbOper = MediaServerOper(_db)
            logger.info("开始同步媒体库数据 ...")
            # 汇总统计
            total_count = 0
            # 清空登记薄
            _dbOper.empty(server=settings.MEDIASERVER)
            for library in self.librarys():
                logger.info(f"正在同步媒体库 {library.name} ...")
                library_count = 0
                for item in self.items(library.id):
                    if not item:
                        continue
                    if not item.item_id:
                        continue
                    # 计数
                    library_count += 1
                    seasoninfo = {}
                    # 类型
                    item_type = "电视剧" if item.item_type in ['Series', 'show'] else "电影"
                    if item_type == "电视剧":
                        # 查询剧集信息
                        espisodes_info = self.episodes(item.item_id) or []
                        for episode in espisodes_info:
                            seasoninfo[episode.season] = episode.episodes
                    # 插入数据
                    item_dict = item.dict()
                    item_dict['seasoninfo'] = json.dumps(seasoninfo)
                    item_dict['item_type'] = item_type
                    _dbOper.add(**item_dict)
                logger.info(f"媒体库 {library.name} 同步完成，共同步数量：{library_count}")
                # 总数累加
                total_count += library_count
            # 关闭数据库连接
            if _db:
                _db.close()
            logger.info("【MediaServer】媒体库数据同步完成，同步数量：%s" % total_count)

import logging
import threading
from uuid import uuid4

import redis
import quickfix as fix
import quickfix44 as fix44
from tornado import gen, ioloop

logger = logging.getLogger(__name__)
# handler = logging.StreamHandler()
# handler.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(handler)
# logger.propagate = False


class Application(fix.Application):

    def __init__(self):
        super().__init__()
        self.sessionID = None
        self.session_off = True
        self.io_loop = ioloop.IOLoop.current()
        self.callback = ioloop.PeriodicCallback(self.send, 10)
        self.callback.start()

    def onCreate(self, sessionID):
        self.sessionID = sessionID
        logger.info(f'onCreate sessionID: [{sessionID.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')

    def onLogon(self, sessionID):
        logger.info(f'onLogon sessionID: [{sessionID.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')
        self.session_off = False

    def onLogout(self, sessionID):
        logger.info(f'onLogout sessionID: [{sessionID.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')
        self.session_off = True

    def toAdmin(self, message, sessionID):
        logger.info(f'toAdmin sessionID: [{sessionID.toString()}], message: [{message.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')

    def fromAdmin(self, message, sessionID):
        logger.info(f'fromAdmin sessionID: [{sessionID.toString()}], message: [{message.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')

    def toApp(self, message, sessionID):
        logger.info(f'toApp sessionID: [{sessionID.toString()}], message: [{message.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')
        if not fix.Session.lookupSession(self.sessionID).isLoggedOn():
            raise fix.DoNotSend()

    def fromApp(self, message, sessionID):
        logger.info(f'fromApp sessionID: [{sessionID.toString()}], message: [{message.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')

    def send(self):
        if not fix.Session.lookupSession(self.sessionID).isLoggedOn():
            return
        # if self.session_off:
        #     return
        logger.info(f'send  main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')
        quote = fix44.Quote()
        quote.setField(fix.QuoteID(str(uuid4())))
        self.io_loop.add_callback(self.send_to_target, quote)

    def send_to_target(self, message):
        logger.info(f'send_to_target  main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')
        try:
            logger.info(fix.Session.sendToTarget(message, self.sessionID))
        except:
            logger.exception('unknown error')


def connect():
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    return redis.StrictRedis(connection_pool=pool)


if __name__ == '__main__':
    settings = fix.SessionSettings('./acceptor.cfg')
    application = Application()
    storeFactory = fix.FileStoreFactory('./acceptor/store')
    logFactory = fix.FileLogFactory('./acceptor/log')
    acceptor = fix.SocketAcceptor(application, storeFactory, settings, logFactory)
    acceptor.start()
    ioloop.IOLoop.current().start()
    # conn = connect()
    # conn.set('fuck', 'moge')
    # print(conn.get('fuck'))

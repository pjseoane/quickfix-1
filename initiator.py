import logging
import threading

import quickfix as fix
from tornado import gen, ioloop

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s')

class Application(fix.Application):

    def __init__(self):
        super().__init__()
        self.sessionID = None
        self.session_off = True
        self.io_loop = ioloop.IOLoop.current()

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

    def fromApp(self, message, sessionID):
        logger.info(f'fromApp sessionID: [{sessionID.toString()}], message: [{message.toString()}], main: [{threading.main_thread().ident}], current[{threading.current_thread().ident}]')


if __name__ == '__main__':
    settings = fix.SessionSettings('./initiator.cfg')
    application = Application()
    storeFactory = fix.FileStoreFactory('./initiator/store')
    logFactory = fix.FileLogFactory('./initiator/log')
    initiator = fix.SocketInitiator(application, storeFactory, settings, logFactory)
    initiator.start()
    ioloop.IOLoop.current().start()

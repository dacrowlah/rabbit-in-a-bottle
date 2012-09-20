import pika
import inspect

class RabbitMQPlugin(object):
    name = 'rabbit'

    def __init__(self,host='localhost', keyword='rbt'):
      self.host = host
      self.keyword = keyword
      self.connection = None

    def setup(self,app):
        for other in app.plugins:
            if not isinstance(other,RabbitMQPlugin): continue
            if other.keyword == self.keyword:
                raise PluginError("Found another rabbit plugin with conflicting settings (non-unique keyword).")

        if self.connection is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))

    def apply(self,callback,context):
        conf = context['config'].get('rabbit') or {}
        args = inspect.getargspec(context['callback'])[0]
        keyword = conf.get('keyword',self.keyword)
        if keyword not in args:
            return callback

        def wrapper(*args,**kwargs):
            kwargs[self.keyword] = pika.BlockingConnection(pika.ConnectionParameters(self.host)))
            rv = callback(*args, **kwargs)
            return rv
        return wrapper

Plugin = RabbitMQPlugin

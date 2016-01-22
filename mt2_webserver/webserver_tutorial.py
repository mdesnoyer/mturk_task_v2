from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response


def hello_world(request):                                       # this function is a 'view callable,' which always accepts a request and returns a response
    return Response('Hello %(name)s!' % request.matchdict)      # an instance of the response class

if __name__ == '__main__':
    config = Configurator()
    config.add_route('hello', '/hello/{name}')                  # what is a 'route' and why do they need to be added?
    config.add_view(hello_world, route_name='hello')            # adds the view for this route, I think.
    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', 8080, app)                  # 0.0.0.0 means 'listen all all TCP interfaces' -- default is 127.0.0.1
    server.serve_forever()
from functools import wraps
import json
import os.path
import traceback

import tornado.web
import tornado.gen
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import time

from minion.app import app
from minion.config import config
import minion.helpers as h
from minion.logger import logger
from minion.logger import cmd_logger
from minion.subprocess.manager import manager
from minion.templates import loader


@h.route(app, r'/ping/')
class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('OK')

MAX_WORKERS = 4

class AuthenticationRequestHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    @run_on_executor
    def background_task(self, task_to_run):
        """ This will be executed in `executor` pool. """
        time.sleep(20)
        result = task_to_run()
        return result

    def is_authenticated(self):
        if config['common']['debug'] == 'True':
            return True
        return (self.request.headers.get('X-Auth', '') ==
                config['auth']['key'])

    @staticmethod
    def auth_required(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            if not self.is_authenticated():
                self.set_status(403)
                return
            return method(self, *args, **kwargs)
        return wrapped


def api_response(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        try:
            self.add_header('Content-Type', 'text/json')
            try:
                res = method(self, *args, **kwargs)
            except Exception as e:
                logger.error('{0}: {1}'.format(e, traceback.format_exc(e)))
                response = {'status': 'error',
                            'error': str(e)}
            else:
                response = {'status': 'success',
                            'response': res}
            try:
                self.write(json.dumps(response))
            except Exception as e:
                logger.error('Failed to dump json response: {0}\n{1}'.format(e,
                    traceback.format_exc(e)))
                response = {'status': 'error',
                            'error': 'failed to construct response, see log file'}
                self.write(json.dumps(response))

        finally:
            self.finish()

    return wrapped


@h.route(app, r'/command/start/', name='start')
@h.route(app, r'/rsync/start/')
class RsyncStartHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornade.gen.coroutine
    def post(self):
        cmd = self.get_argument('command')
        success_codes = [int(c) for c in self.get_arguments('success_code')]
        params = dict((k, v[0]) for k, v in self.request.arguments.iteritems()
                                if k not in ('success_code',))
        env = {}
        for header, value in self.request.headers.iteritems():
            if header.upper().startswith('ENV_'):
                env_var_name = header.upper()[len('ENV_'):]
                env[env_var_name] = value
        uid = yield self.background_task(manager.run(cmd, params, env=env, success_codes=success_codes))
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/rsync/manual/')
class RsyncManualHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        response = yield loader.load('manual.html').generate()
        self.write(response)


@h.route(app, r'/command/terminate/', name='terminate')
class CommandTerminateHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def post(self):
        uid = self.get_argument('cmd_uid')
        yield self.background_task(manager.terminate(uid))
        status = yield self.background_task(manager.status(uid))
        return {uid: status}


@h.route(app, r'/command/status/([0-9a-f]+)/', name='status')
@h.route(app, r'/rsync/status/([0-9a-f]+)/')
class RsyncStatusHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def get(self, uid):
        status = yield self.background_task(manager.status(uid))
        return {uid: status}


@h.route(app, r'/node/shutdown/', name='node_shutdown')
class NodeShutdownHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def post(self):
        cmd = self.get_argument('command')
        params = dict((k, v[0]) for k, v in self.request.arguments.iteritems())
        uid = yield self.background_task(manager.run(cmd, params))
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/command/list/')
@h.route(app, r'/rsync/list/')
class RsyncListHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def get(self):
        finish_ts_gte = int(self.get_argument('finish_ts_gte', default=0)) or None
        result = yield self.background_task(manager.unfinished_commands(finish_ts_gte=finish_ts_gte))

        # NOTE: filtering out stdout and stderr since mastermind does not use them
        for command in result.itervalues():
            del command['output']
            del command['error_output']

        return result


@h.route(app, r'/command/create_group/')
class CreateGroupHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def post(self):
        params = {
            k: v[0]
            for k, v in self.request.arguments.iteritems()
        }
        log_extra = {
            'task_id': params.get('task_id'),
            'job_id': params.get('job_id'),
        }
        if config['common'].get('base_path') is None:
            cmd_logger.error('base path is not set, create group cannot be performed', extra=log_extra)
            self.set_status(500)
            raise RuntimeError('group creation is not allowed')
        files = {}
        for filename, http_files in self.request.files.iteritems():
            norm_filename = os.path.normpath(filename)
            if norm_filename.startswith('..') or norm_filename.startswith('/'):
                cmd_logger.error(
                    'Cannot create file {filename}, '
                    'normalized path {norm_filename} is not allowed'.format(
                        filename=filename,
                        norm_filename=norm_filename,
                    ),
                    extra=log_extra,
                )
                self.set_status(403)
                raise RuntimeError(
                    'File {filename} is forbidden, path should be relative '
                    'to group base directory'.format(filename=filename)
                )
            http_file = http_files[0]
            files[norm_filename] = http_file.body

        params['group_base_path_root_dir'] = os.path.normpath(params['group_base_path_root_dir'])
        if not params['group_base_path_root_dir'].startswith(config['common']['base_path']):
            self.set_status(403)
            raise RuntimeError(
                'Group base path {path} is not under common base path'.format(
                    path=params['group_base_path_root_dir'],
                )
            )
        params['files'] = files
        uid = yield self.background_task(manager.run('create_group', params=params))
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/command/remove_group/')
class RemoveGroupHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def post(self):
        params = {
            k: v[0]
            for k, v in self.request.arguments.iteritems()
        }
        if config['common'].get('base_path') is None:
            cmd_logger.error('base path is not set, remove group cannot be performed', extra={
                'task_id': params.get('task_id'),
                'job_id': params.get('job_id'),
            })
            self.set_status(500)
            raise RuntimeError('group creation is not allowed')
        params['group_base_path'] = os.path.normpath(params['group_base_path'])
        if not params['group_base_path'].startswith(config['common']['base_path']):
            self.set_status(403)
            raise RuntimeError(
                'Group path {path} is not under common base path'.format(
                    path=params['group_base_path'],
                )
            )
        uid = yield self.background_task(manager.run('remove_group', params=params))
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/command/(.+)/')
class CmdHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    @tornado.gen.coroutine
    def post(self, cmd):
        params = {
            k: v[0]
            for k, v in self.request.arguments.iteritems()
        }
        uid = yield self.background_task(manager.run(cmd, params=params))
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))

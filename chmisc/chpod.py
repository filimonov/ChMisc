from podman import PodmanClient
import logging
from io import BytesIO
import requests
import tarfile
import os
from time import sleep
from pkg_resources import packaging



class ChPod(object):

    podman = PodmanClient()
    logger = logging.getLogger('ChInstance')

    def __init__(self, image):
        self.logger.debug('Instance creation')
        self.image = f'docker.io/{image}'
        self.name = None
        self.container = None
        self.http_url = None
        self.version = None
        self.__container_name()
        self.__start_container()


    def __del__(self):
        self.logger.debug('Destroying instance ' + self.__repr__())
        if self.container is not None:
            self.container.stop()
        self.logger.debug('Removing image ' + self.image)
        if self.podman.images.exists(self.image):
            client.images.remove(self.image)


    def __get_clickhouse_http_url(self):
        if self.http_url is None:
            clickhouse_http_port = self.container.ports['8123/tcp'][0]['HostPort']
            self.http_url = f'http://127.0.0.1:{clickhouse_http_port}/'
            self.logger.info(f'ClickHouse http url: {self.http_url}')
        return self.http_url

    def __container_name(self):
        self.name = 'clickhouse_' + self.image.split(':')[1]

    def __start_container(self):
        if not self.podman.images.exists(self.image):
            self.logger.info(f'Image {self.image} not in local repository, fetching')
            if not self.podman.images.pull(self.image):
                self.logger.error(f'Failure to fetch image {self.image}, impossible to start container')
                raise Exception(f'{self.image} cannot be fetched from remote repository, aborting.')
            else:
                self.logger.info(f'Image {self.image} fetched')

        if self.podman.containers.exists(self.name):
            self.logger.warning(f'Container {self.name} exists, stop/removing and recreating/starting')
            self.container = self.podman.containers.get(self.name)
            self.container.stop()
        self.logger.info(f'Creating container {self.name} with image {self.image}')
        self.container = self.podman.containers.create(
                self.image,
                auto_remove=True,
                detach=True,
                hostname='clickhouse',
                name=self.name,
                ports={
                    f'8123/tcp': None # let podman choose a random port
                },
                mounts=[
                    {
                        "type": "bind",
                        "source": f'{__file__}/../../volumes/config.d/network.xml',
                        "target": "/etc/clickhouse-server/config.d/network.xml",
                        "read_only": True,
                        "relabel": "Z"
                    }
                ]
        )
        self.container.start()
        self.container.wait(condition='running')
        self.logger.info(f'Container {self.name} started')
        if not self.__health_check():
            raise Exception(f'Liveness check failed for {self.name}: cannot connect/execute liveness query!')

    def __health_check(self):
        tries = 0
        while True:
            self.logger.info(f'Liveness check attempt {tries+1}')
            try:
                ok, resp = self.query("SELECT 1")
                if not ok:
                    raise Exception(f'HTTP Error {resp.status_code}: {resp.text}')
                return True
            except Exception as e:
                tries += 1
                if tries > 10:
                    self.logger.warning(f'Liveness check failed after {tries} attempts, timing out')
                    return False
                sleep(3)
                continue

    def query(self, query, extra_params=None, extra_headers=None):
        headers = {}
        # headers["X-ClickHouse-User"] = ...
        # headers["X-ClickHouse-Key"] = ...

        if extra_headers is not None:
            headers.update(extra_headers)

        params = {
            "database": "default",
            "query": query,
        }

        if extra_params is not None:
            params.update(extra_params)

        response = requests.post(url=self.__get_clickhouse_http_url(),
                         params=params,
                         headers=headers)

        # if response.status_code != 200:
        #     raise Exception(f'HTTP Error {response.status_code}: {response.text}')

        return response.status_code == 200, response.text


    def get_version(self):
        if self.version is None:
            status, version = self.query('SELECT version() AS ch_version FORMAT TabSeparated')
            if not status:
                self.logger.error(f'Failure to get version from {image}, skipping')
                return None
            self.version = version.strip()
        return self.version


    def is_version_newer_than(self, version):
        return packaging.version.parse(self.get_version()) >= packaging.version.parse(version)

    def __get_clickhouse_path(self) -> str:
        # without tty = True it returns some trash prefix, see https://stackoverflow.com/q/77348891/1555175
        exit_code, output = self.container.exec_run('clickhouse-extract-from-config --config=/etc/clickhouse-server/config.xml --key=path', tty=True)
        if exit_code != 0:
            error_msg = output.decode('utf-8').strip()
            self.logger.error(f"Error getting clickhouse path for {self.name}: Exit Code {exit_code}, Output: {error_msg}")
            return '/var/lib/clickhouse'
        return output.decode('utf-8').strip()

    def __get_preprocessed_configs_path(self) -> str:
        # https://github.com/ClickHouse/ClickHouse/commit/f1791e94e209b238b28a89c25e3a5f81882785a6
        if self.is_version_newer_than("18.16.0"):
            clickhouse_path = self.__get_clickhouse_path()
            return os.path.join(clickhouse_path, 'preprocessed_configs')
        else:
            return '/etc/clickhouse-server/'

    def get_preprocessed_configs(self) -> dict:
        p = self.__get_preprocessed_configs_path()
        self.logger.info(f'Extracting preprocessed configs from {self.name} ({p})')
        bits, stat = self.container.get_archive(p)

        files = {}

        with BytesIO() as f:
            for chunk in bits:
                f.write(chunk)
            f.seek(0)

            with tarfile.open(fileobj=f, mode='r') as tar:
                for member in tar.getmembers():
                    if member.name.endswith('.xml'):

                        ## if member name contains preprocessed_configs or if it have a -preprocessed.xml suffix
                        ## then it is a preprocessed config file
                        if 'preprocessed_configs' in member.name or member.name.endswith('-preprocessed.xml'):
                            with tar.extractfile(member) as file:
                                if file:
                                    self.logger.info(f'Found file {member.name}')

                                    # extract file name from the path
                                    key = os.path.basename(member.name)

                                    # remove -preprocessed.xml suffix if it exists
                                    key = key.replace('-preprocessed.xml', '.xml')

                                    self.logger.info(f'Will store info into {key}')
                                    files[key] = file.read().decode()

        return files

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name} - {self.image})'

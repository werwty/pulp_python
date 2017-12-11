from collections import namedtuple
from gettext import gettext as _
import json
from logging import getLogger
from urllib.parse import urljoin

from django.contrib.postgres.fields import ArrayField
from django.db import models

from pulpcore.plugin.models import (Artifact, Content, ContentArtifact, RemoteArtifact, Importer,
                                    ProgressBar, Publisher, RepositoryContent, PublishedArtifact,
                                    PublishedMetadata)

from pulpcore.plugin.changeset import (
    BatchIterator,
    ChangeSet,
    PendingArtifact,
    PendingContent,
    SizedIterable,
)


log = getLogger(__name__)

Delta = namedtuple('Delta', ('additions', 'removals'))


class Classifier(models.Model):
    """
    Custom tags for classifier

    Fields:

        name (models.TextField): The name of the classifier

    Relations:

        python_package_content (models.ForeignKey): The PythonPackageContent this classifier
        is associated with.

    """

    name = models.TextField()
    python_package_content = models.ForeignKey("PythonPackageContent", related_name="classifiers",
                                               related_query_name="classifier")


class PythonPackageContent(Content):
    """
    A Content Type representing Python's Distribution Package as
    defined in pep-0426 and pep-0345
    https://www.python.org/dev/peps/pep-0491/
    https://www.python.org/dev/peps/pep-0345/
    """

    TYPE = 'python'
    filename = models.TextField(unique=True, db_index=True, blank=False)
    packagetype = models.TextField(blank=False)
    name = models.TextField(blank=False)
    version = models.TextField(blank=False)
    metadata_version = models.TextField(blank=False)
    summary = models.TextField(null=True, max_length=8192)
    description = models.TextField(null=True, max_length=8192)
    keywords = models.TextField(null=True)
    home_page = models.TextField(null=True)
    download_url = models.TextField(null=True)
    author = models.TextField(null=True)
    author_email = models.TextField(null=True)
    maintainer = models.TextField(null=True)
    maintainer_email = models.TextField(null=True)
    license = models.TextField(null=True)
    requires_python = models.TextField(null=True)
    project_url = models.TextField(null=True)
    platform = models.TextField(null=True)
    supported_platform = models.TextField(null=True)
    requires_dist = ArrayField(models.TextField())
    provides_dist = ArrayField(models.TextField())
    obsoletes_dist = ArrayField(models.TextField())
    requires_external = ArrayField(models.TextField())


class PythonPublisher(Publisher):
    """
    A Publisher for PythonContent.

    Define any additional fields for your new publisher if needed.
    A ``publish`` method should be defined.
    It is responsible for publishing metadata and artifacts
    which belongs to a specific repository.
    """

    TYPE = 'python'

    def publish(self):
        """
        Publish the repository.
        """
        raise NotImplementedError


class PythonImporter(Importer):
    """
    An Importer for PythonContent.

    Define any additional fields for your new importer if needed.
    A ``sync`` method should be defined.
    It is responsible for parsing metadata of the content,
    downloading of the content and saving it to Pulp.
    """

    TYPE = 'python'
    project_names = ArrayField(models.TextField())

    def _fetch_inventory(self):
        """
        Fetch existing content in the repository.
        Returns:
            set: of content filenames.
        """
        inventory = set()

        q_set = PythonPackageContent.objects.filter(repositories=self.repository)
        q_set = q_set.only("filename")
        for content in (c.cast() for c in q_set):
            inventory.add(content.filename)
        return inventory

    def _fetch_remote(self):

        remote = []

        metadata_urls = [urljoin(self.feed_url, 'pypi/%s/json' % project)
                         for project in self.project_names]

        for metadata_url in metadata_urls:
            download = self.get_asyncio_downloader(metadata_url)
            download_result = download.fetch()

            metadata = json.load(open(download_result.path))
            for version, packages in metadata['releases'].items():
                for package in packages:
                    remote.append(self._parse_package(metadata['info'], version, package))

        return remote

    def _find_delta(self, inventory, remote, mirror=True):
        remote_keys = set([content['filename'] for content in remote])

        additions_set = remote_keys - inventory
        additions = [content for content in remote if content['filename'] in additions_set]
        if mirror:
            removals_set = inventory - remote_keys
            removals = [content for content in remote if content['filename'] in removals_set]

        else:
            removals = []

        return Delta(additions=additions, removals=removals)

    def _parse_package(cls, project, version, distribution):

        package_attrs = {}

        package_attrs['filename'] = distribution['filename']
        package_attrs['packagetype'] = distribution['packagetype']
        package_attrs['name'] = project['name']
        package_attrs['version'] = version
        package_attrs['metadata_version'] = '3.0'
        package_attrs['summary'] = project.get('summary')
        package_attrs['description'] = project.get('description')
        package_attrs['keywords'] = project.get('keywords')
        package_attrs['home_page'] = project.get('home_page')
        package_attrs['download_url'] = project.get('download_url')
        package_attrs['author'] = project.get('author')
        package_attrs['author_email'] = project.get('author_email')
        package_attrs['maintainer'] = project.get('maintainer')
        package_attrs['maintainer_email'] = project.get('maintainer_email')
        package_attrs['license'] = project.get('license')
        package_attrs['requires_python'] = project.get('requires_python')
        package_attrs['project_url'] = project.get('project_url')
        package_attrs['platform'] = project.get('platform')
        package_attrs['supported_platform'] = project.get('supported_platform')
        package_attrs['requires_dist'] = project.get('requires_dist', [])
        package_attrs['provides_dist'] = project.get('provides_dist', [])
        package_attrs['obsoletes_dist'] = project.get('obsoletes_dist', [])
        package_attrs['requires_external'] = project.get('requires_external', [])
        package_attrs['url'] = distribution['url']
        package_attrs['digest'] = distribution['digests'].get('sha256')

        return package_attrs

    def _build_additions(self, additions):
        for entry in additions:
            # Instantiate the content and artifact based on the manifest entry.

            url = entry['url']
            artifact = Artifact(sha256=entry['digest'])

            entry.pop('url')
            entry.pop('digest')

            package = PythonPackageContent(**entry)
            content = PendingContent(
                package,
                artifacts={
                    PendingArtifact(model=artifact, url=url, relative_path=entry['filename'])
                })
            yield content

    def sync(self):

        inventory = self._fetch_inventory()
        remote = self._fetch_remote()

        delta = self._find_delta(inventory=inventory, remote=remote)
        additions = SizedIterable(
            self._build_additions(delta.additions),
            len(delta.additions))

        changeset = ChangeSet(self, additions=additions)
        changeset.apply_and_drain()


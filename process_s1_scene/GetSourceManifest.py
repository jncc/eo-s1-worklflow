import luigi
import os
import json
import logging
import process_s1_scene.common as wc
from string import Template

log = logging.getLogger('luigi-interface')

class GetSourceManifest(luigi.ExternalTask):

    def output(self):
        return wc.getLocalStateTarget(self.pathRoots["processingDir"], "manifest.safe")
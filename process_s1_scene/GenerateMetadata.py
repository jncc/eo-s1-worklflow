import luigi
import os
import json
import logging
import process_s1_scene.common as wc
import uuid
import datetime
from luigi.util import requires
from process_s1_scene.AddMergedOverviews import AddMergedOverviews
from string import Template

log = logging.getLogger('luigi-interface')

@requires(AddMergedOverviews)
class GenerateMetadata(luigi.Task):
    pathRoots = luigi.DictParameter()
    productId = luigi.Parameter()
    sourceFile = luigi.Parameter()

    def run(self):
        spec = {}
        with self.input().open('r') as inputFile:
            spec = json.loads(inputFile.read())

        dateToday = datetime.date.today()

        metadataParams = {
            "uuid": uuid.uuid4(),
            "metadataDate": str(dateToday),
            "publishedDate": str(dateToday),
            "extentWestBound": None,
            "extentEastBound": None,
            "extentSouthBound": None,
            "extentNorthBound": None,
            "extentStartDate": wc.getStartDateFromSourceFile(self.sourceFile),
            "extentEndDate": wc.getEndDateFromSourceFile(self.sourceFile),
            "datasetVersion": "v1.0",
            "projection": wc.getProjectionFromOutputFile(spec["files"]["VV"][0]),
            "polarisation": "VV+VH",
            "collectionMode": wc.getCollectionModeFromSourceFile(self.sourceFile)
        }

        with open("process_s1_scene/metadata_template/s1_metadata_template.xml", "r") as templateFile:
            template = Template(templateFile.read())
            metadataString = template.substitute(metadataParams)

        metadataFilepath = os.path.join(os.path.join(os.path.join(self.pathRoots["fileRoot"], "output"), spec["productPattern"]), "metadata.xml")
        with open(metadataFilepath, 'w') as out:
            out.write(metadataString)

        with self.output().open('w') as out:
            out.write("metadata generated")

    def output(self):
        outputFolder = os.path.join(self.pathRoots["state-localRoot"], self.productId)
        return wc.getLocalStateTarget(outputFolder, 'generateMetadata.json')
import asyncio
import random
import logging
from typing import List
from .mock_agents import (
    mock_mysql_create_project,
    mock_nfs_hot_add_file,
    mock_nfs_cold_add_file,
    mock_oss_add_dataset_link,
    mock_es_add_publication
)

logger = logging.getLogger("fustor_demo.generator")

# Sample data pools for random generation
PROJECT_NAMES = [
    "Cancer_Genomics_2024", "Viral_Study_Beta", "Plant_Diversity_X", 
    "Rare_Disease_Gamma", "Microbiome_Gut_Health", "Neuro_Science_Alpha"
]

FILE_PREFIXES = ["seq_run", "analysis_result", "raw_image", "patient_data", "gene_expression"]
FILE_EXTENSIONS = [".fastq", ".bam", ".vcf", ".tiff", ".csv", ".json"]

DATASET_NAMES = ["1000_Genomes", "TCGA_Public", "GSA_Reference", "OMIX_Baseline", "UniProt_Dump"]

TITLES = [
    "Novel findings in gene X", "Study of protein Y interaction", 
    "Large scale population analysis", "Review of viral vectors", "Methodology for fast sequencing"
]

class AutoDataGenerator:
    def __init__(self, interval_min: float = 2.0, interval_max: float = 5.0):
        self.interval_min = interval_min
        self.interval_max = interval_max
        self._running = False
        self._task = None
        self._generated_projects: List[str] = [] # Keep track of created projects to add files to them

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Auto data generator started.")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Auto data generator stopped.")

    async def _loop(self):
        # Initial seeding: Create a couple of projects
        for name in PROJECT_NAMES[:2]:
            self._create_project(name)
            await asyncio.sleep(0.5)

        while self._running:
            try:
                # Pick a random action
                action_type = random.choice([
                    "create_project", 
                    "add_nfs_hot", "add_nfs_hot", "add_nfs_hot", # Higher weight for files
                    "add_nfs_cold", 
                    "add_oss", 
                    "add_es"
                ])

                if action_type == "create_project":
                    # Create a new project from the list if not all exist, or a random variant
                    available_names = [p for p in PROJECT_NAMES if p.lower() not in self._generated_projects]
                    if available_names:
                        self._create_project(random.choice(available_names))
                    else:
                        # Create a variant
                        base = random.choice(PROJECT_NAMES)
                        variant = f"{base}_v{random.randint(1, 99)}"
                        self._create_project(variant)

                elif self._generated_projects: # Only add items if we have projects
                    project_id = random.choice(self._generated_projects)
                    
                    if action_type == "add_nfs_hot":
                        fname = f"{random.choice(FILE_PREFIXES)}_{random.randint(100, 999)}{random.choice(FILE_EXTENSIONS)}"
                        size = random.randint(10, 5000)
                        mock_nfs_hot_add_file(project_id, fname, size)
                        logger.info(f"Auto-generated NFS Hot file '{fname}' for '{project_id}'")

                    elif action_type == "add_nfs_cold":
                        fname = f"archive_{random.choice(FILE_PREFIXES)}_{random.randint(2010, 2023)}{random.choice(FILE_EXTENSIONS)}"
                        size = random.randint(50, 200) # GB
                        mock_nfs_cold_add_file(project_id, fname, size)
                        logger.info(f"Auto-generated NFS Cold file '{fname}' for '{project_id}'")

                    elif action_type == "add_oss":
                        ds_name = f"{random.choice(DATASET_NAMES)}_{random.randint(1, 10)}.tar.gz"
                        mock_oss_add_dataset_link(project_id, ds_name, f"https://oss.example.com/{ds_name}")
                        logger.info(f"Auto-generated OSS link '{ds_name}' for '{project_id}'")

                    elif action_type == "add_es":
                        title = f"{random.choice(TITLES)} - {random.randint(1, 100)}"
                        pid = str(random.randint(10000000, 99999999))
                        mock_es_add_publication(project_id, title, pid)
                        logger.info(f"Auto-generated ES citation '{title}' for '{project_id}'")

                # Sleep for random interval
                delay = random.uniform(self.interval_min, self.interval_max)
                await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"Error in auto generator loop: {e}")
                await asyncio.sleep(5) # Retry delay

    def _create_project(self, name: str):
        mock_mysql_create_project(name)
        project_id = name.lower().replace(" ", "_")
        if project_id not in self._generated_projects:
            self._generated_projects.append(project_id)
        logger.info(f"Auto-generated Project '{name}'")

# Global instance
generator = AutoDataGenerator()
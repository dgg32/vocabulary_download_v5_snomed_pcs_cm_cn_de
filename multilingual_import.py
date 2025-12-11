#!/usr/bin/env python3
"""
Build Neo4j graph - Import ALL concepts with any target language name
Correct logic: SYNONYM → find concepts → map to SNOMED → create nodes
"""

import pandas as pd
from neo4j import GraphDatabase
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "YOUR_NEO4J_PASSWORD_HERE"

# Data directory
DATA_DIR = "/Users/dgg32/Documents/claude/vocabulary_download_v5_snomed_pcs_cm_cn_de"

# Domains to exclude (not relevant for medical dictionary)
EXCLUDED_DOMAINS = ['Geography']  # Add more if needed: ['Geography', 'Type Concept', 'Metadata']


class MultilingualNeo4jGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Remove all existing nodes and relationships"""
        logger.info("Clearing existing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared")
    
    def create_constraints(self):
        """Create unique constraints"""
        logger.info("Creating constraints...")
        with self.driver.session() as session:
            session.run("""
                CREATE CONSTRAINT concept_id_unique IF NOT EXISTS
                FOR (c:Concept) REQUIRE c.concept_id IS UNIQUE
            """)
        logger.info("Constraints created")
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        logger.info("Creating indexes...")
        with self.driver.session() as session:
            session.run("""
                CREATE INDEX concept_code_idx IF NOT EXISTS
                FOR (c:Concept) ON (c.concept_code)
            """)
            session.run("""
                CREATE INDEX vocabulary_id_idx IF NOT EXISTS
                FOR (c:Concept) ON (c.vocabulary_id)
            """)
            session.run("""
                CREATE INDEX canonical_name_idx IF NOT EXISTS
                FOR (c:Concept) ON (c.canonical_name)
            """)
            # session.run("""
            #     CREATE INDEX name_value_idx IF NOT EXISTS
            #     FOR (n:Name) ON (n.value)
            # """)
            session.run("""
                CREATE INDEX name_language_idx IF NOT EXISTS
                FOR (n:Name) ON (n.language_concept_id)
            """)
            session.run("""
                CREATE FULLTEXT INDEX name_fulltext IF NOT EXISTS
                FOR (n:Name) ON EACH [n.value]
            """)
        logger.info("Indexes created")
    
    def batch_create_concepts(self, concepts_df):
        """Create concept nodes in batches"""
        logger.info(f"Creating {len(concepts_df)} concept nodes...")
        
        batch_size = 1000
        total_batches = (len(concepts_df) + batch_size - 1) // batch_size
        
        with self.driver.session() as session:
            for i in range(0, len(concepts_df), batch_size):
                batch = concepts_df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                concepts = batch.to_dict('records')
                
                session.run("""
                    UNWIND $concepts AS concept
                    CREATE (c:Concept {
                        concept_id: concept.concept_id,
                        concept_code: concept.concept_code,
                        canonical_name: concept.canonical_name,
                        domain_id: concept.domain_id,
                        vocabulary_id: concept.vocabulary_id,
                        concept_class_id: concept.concept_class_id,
                        standard_concept: concept.standard_concept
                    })
                """, concepts=concepts)
                
                if batch_num % 10 == 0:
                    logger.info(f"  Processed batch {batch_num}/{total_batches}")
        
        logger.info("All concept nodes created")
    
    def batch_create_names(self, names_df):
        """Create Name nodes and connect to Concepts"""
        logger.info(f"Creating {len(names_df)} name nodes and HAS_NAME relationships...")
        
        batch_size = 1000
        total_batches = (len(names_df) + batch_size - 1) // batch_size
        
        with self.driver.session() as session:
            for i in range(0, len(names_df), batch_size):
                batch = names_df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                names = batch.to_dict('records')
                
                session.run("""
                    UNWIND $names AS name
                    MATCH (c:Concept {concept_id: name.concept_id})
                    CREATE (n:Name {
                        concept_id: name.concept_id,
                        value: name.value,
                        language_concept_id: name.language_concept_id,
                        language_name: name.language_name
                    })
                    CREATE (c)-[:HAS_NAME]->(n)
                """, names=names)
                
                if batch_num % 10 == 0:
                    logger.info(f"  Processed batch {batch_num}/{total_batches}")
        
        logger.info("All name nodes and relationships created")
    
    def batch_create_relationships(self, relationships_df, relationship_type):
        """Create relationships in batches"""
        if len(relationships_df) == 0:
            logger.info(f"No {relationship_type} relationships to create")
            return
        
        logger.info(f"Creating {len(relationships_df)} {relationship_type} relationships...")
        
        batch_size = 1000
        total_batches = (len(relationships_df) + batch_size - 1) // batch_size
        
        with self.driver.session() as session:
            for i in range(0, len(relationships_df), batch_size):
                batch = relationships_df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                relationships = batch.to_dict('records')
                
                session.run(f"""
                    UNWIND $relationships AS rel
                    MATCH (source:Concept {{concept_id: rel.concept_id_1}})
                    MATCH (target:Concept {{concept_id: rel.concept_id_2}})
                    CREATE (source)-[:{relationship_type}]->(target)
                """, relationships=relationships)
                
                if batch_num % 10 == 0:
                    logger.info(f"  Processed batch {batch_num}/{total_batches}")
        
        logger.info(f"All {relationship_type} relationships created")


def load_languages(data_dir):
    """Load language mapping from language_id.csv"""
    logger.info("Step 1: Loading language mappings...")
    language_file = Path(data_dir) / "language_id.csv"
    
    languages_df = pd.read_csv(language_file)
    language_map = dict(zip(languages_df['language_concept_id'], languages_df['language_name']))
    
    logger.info(f"Target languages: {list(language_map.values())}")
    logger.info(f"Language IDs: {list(language_map.keys())}")
    
    return language_map


def build_graph(data_dir):
    """Main function to build the graph with correct logic"""
    
    data_path = Path(data_dir)
    
    # File paths
    excel_file = data_path / "1.2023年中文版ICD-10-CM_PCS_1131118V3(修改ICD-10-CM之N80.A0等中文名稱).xlsx"
    concept_file = data_path / "CONCEPT_cleaned.csv"
    relationship_file = data_path / "CONCEPT_RELATIONSHIP_cleaned.csv"
    synonym_file = data_path / "CONCEPT_SYNONYM_cleaned.csv"
    
    # Step 1: Load language mappings
    language_map = load_languages(data_dir)
    target_language_ids = set(language_map.keys())
    
    # Step 2: Load CONCEPT_SYNONYM and find ALL concepts with target languages
    logger.info("Step 2: Loading CONCEPT_SYNONYM and finding concepts with target languages...")
    synonyms_df = pd.read_csv(synonym_file, sep='\t')
    
    logger.info(f"Total synonym entries: {len(synonyms_df):,}")
    
    # Filter for target languages
    target_synonyms = synonyms_df[synonyms_df['language_concept_id'].isin(target_language_ids)].copy()
    
    logger.info(f"Synonym entries in target languages: {len(target_synonyms):,}")
    logger.info("Distribution by language:")
    for lang_id, lang_name in language_map.items():
        count = len(target_synonyms[target_synonyms['language_concept_id'] == lang_id])
        logger.info(f"  {lang_name} ({lang_id}): {count:,}")
    
    # Get all concept IDs that have names in target languages
    concepts_with_translations = set(target_synonyms['concept_id'].unique())
    logger.info(f"Unique concepts with target language names: {len(concepts_with_translations):,}")
    
    # Step 3: Load Excel file for ICD-10 codes with Chinese names
    logger.info("Step 3: Loading ICD-10 codes from Excel (for Chinese names)...")
    
    # Read ICD-10-CM
    icd10cm_df = pd.read_excel(excel_file, sheet_name='ICD-10-CM')
    logger.info(f"ICD-10-CM columns: {list(icd10cm_df.columns)}")
    
    # Find columns flexibly
    code_col = [c for c in icd10cm_df.columns if 'ICD' in str(c) and 'CM' in str(c)][0]
    chinese_col = [c for c in icd10cm_df.columns if '中文' in str(c) and 'CM' in str(c).upper()][0]
    
    icd10cm_chinese = {}
    for _, row in icd10cm_df.iterrows():
        code = str(row[code_col]).strip() if pd.notna(row[code_col]) else ''
        if code and code != 'nan' and pd.notna(row[chinese_col]):
            chinese_name = str(row[chinese_col]).strip()
            if chinese_name and chinese_name != 'nan':
                icd10cm_chinese[code] = chinese_name
    
    logger.info(f"ICD-10-CM codes with Chinese: {len(icd10cm_chinese)}")
    
    # Read ICD-10-PCS
    icd10pcs_df = pd.read_excel(excel_file, sheet_name='ICD-10-PCS')
    logger.info(f"ICD-10-PCS columns: {list(icd10pcs_df.columns)}")
    
    code_col = [c for c in icd10pcs_df.columns if 'ICD' in str(c) and 'PCS' in str(c)][0]
    chinese_col = [c for c in icd10pcs_df.columns if '中文' in str(c) and 'PCS' in str(c).upper()][0]
    
    icd10pcs_chinese = {}
    for _, row in icd10pcs_df.iterrows():
        code = str(row[code_col]).strip() if pd.notna(row[code_col]) else ''
        if code and code != 'nan' and pd.notna(row[chinese_col]):
            chinese_name = str(row[chinese_col]).strip()
            if chinese_name and chinese_name != 'nan':
                icd10pcs_chinese[code] = chinese_name
    
    logger.info(f"ICD-10-PCS codes with Chinese: {len(icd10pcs_chinese)}")
    
    # Step 4: Load all concepts
    logger.info("Step 4: Loading CONCEPT file...")
    concepts_df = pd.read_csv(concept_file, sep='\t', dtype={'concept_code': str})
    concepts_df['standard_concept'] = concepts_df['standard_concept'].fillna('')
    
    logger.info(f"Total concepts in CONCEPT file: {len(concepts_df):,}")
    
    # Step 5: Find concepts to import
    logger.info("Step 5: Determining which concepts to import...")
    
    # Strategy: Import ALL concepts that have translations
    concepts_to_import = concepts_with_translations.copy()
    
    logger.info(f"Concepts with target language translations: {len(concepts_to_import):,}")
    
    # Step 6: Load relationships to find related concepts
    logger.info("Step 6: Loading relationships to find additional concepts...")
    relationships_df = pd.read_csv(relationship_file, sep='\t')
    
    # Find all MAPS_TO and IS_A relationships involving our concepts
    related_rels = relationships_df[
        (relationships_df['relationship_id'].isin(['Maps to', 'Is a'])) &
        ((relationships_df['concept_id_1'].isin(concepts_to_import)) |
         (relationships_df['concept_id_2'].isin(concepts_to_import)))
    ].copy()
    
    # Add all concepts in these relationships (one hop)
    additional_concepts = set(related_rels['concept_id_1'].tolist() + related_rels['concept_id_2'].tolist())
    additional_concepts = additional_concepts - concepts_to_import
    
    logger.info(f"Additional concepts via direct relationships: {len(additional_concepts):,}")
    
    # Step 6b: Recursively traverse IS_A hierarchy to get complete chains
    logger.info("Step 6b: Traversing complete IS_A hierarchy...")
    
    # Get all IS_A relationships
    is_a_rels = relationships_df[relationships_df['relationship_id'] == 'Is a'].copy()
    
    # Build lookup dictionaries for faster traversal
    # parents[concept_id] = set of parent concept_ids
    parents = {}
    # children[concept_id] = set of child concept_ids
    children = {}
    
    for _, row in is_a_rels.iterrows():
        child_id = row['concept_id_1']
        parent_id = row['concept_id_2']
        
        if child_id not in parents:
            parents[child_id] = set()
        parents[child_id].add(parent_id)
        
        if parent_id not in children:
            children[parent_id] = set()
        children[parent_id].add(child_id)
    
    # Function to get all ancestors (recursive traversal up)
    def get_all_ancestors(concept_id, visited=None):
        if visited is None:
            visited = set()
        
        if concept_id in visited:
            return visited
        
        visited.add(concept_id)
        
        if concept_id in parents:
            for parent_id in parents[concept_id]:
                get_all_ancestors(parent_id, visited)
        
        return visited
    
    # Function to get all descendants (recursive traversal down)
    def get_all_descendants(concept_id, visited=None):
        if visited is None:
            visited = set()
        
        if concept_id in visited:
            return visited
        
        visited.add(concept_id)
        
        if concept_id in children:
            for child_id in children[concept_id]:
                get_all_descendants(child_id, visited)
        
        return visited
    
    # Collect all concepts in the complete hierarchy
    all_hierarchy_concepts = set()
    
    # For each concept with translations, get complete ancestor and descendant chains
    starting_concepts = concepts_to_import | additional_concepts
    
    for concept_id in starting_concepts:
        # Get all ancestors (parents, grandparents, etc.)
        ancestors = get_all_ancestors(concept_id)
        all_hierarchy_concepts.update(ancestors)
        
        # Get all descendants (children, grandchildren, etc.)
        descendants = get_all_descendants(concept_id)
        all_hierarchy_concepts.update(descendants)
    
    # Remove the starting concepts (we already have them)
    hierarchy_only_concepts = all_hierarchy_concepts - starting_concepts
    
    logger.info(f"Additional concepts from complete hierarchy traversal: {len(hierarchy_only_concepts):,}")
    
    # Combine all concepts
    all_concept_ids = concepts_to_import | additional_concepts | hierarchy_only_concepts
    
    logger.info(f"Total concepts to import: {len(all_concept_ids):,}")
    
    # Step 7: Filter CONCEPT dataframe
    logger.info("Step 7: Filtering CONCEPT data...")
    final_concepts = concepts_df[concepts_df['concept_id'].isin(all_concept_ids)].copy()
    final_concepts['canonical_name'] = final_concepts['concept_name']
    
    # Remove duplicates
    final_concepts = final_concepts.drop_duplicates(subset=['concept_id'])
    
    # Filter out excluded domains
    excluded_concepts = final_concepts[final_concepts['domain_id'].isin(EXCLUDED_DOMAINS)]
    logger.info(f"Filtering out {len(excluded_concepts)} concepts from excluded domains: {EXCLUDED_DOMAINS}")
    
    for domain in EXCLUDED_DOMAINS:
        count = len(excluded_concepts[excluded_concepts['domain_id'] == domain])
        logger.info(f"  {domain}: {count:,} concepts")
    
    final_concepts = final_concepts[~final_concepts['domain_id'].isin(EXCLUDED_DOMAINS)].copy()
    
    # Update concept IDs set
    excluded_concept_ids = set(excluded_concepts['concept_id'].tolist())
    all_concept_ids = all_concept_ids - excluded_concept_ids
    
    logger.info(f"Final concept count after Geography filter: {len(final_concepts)}")
    logger.info("By vocabulary:")
    for vocab in final_concepts['vocabulary_id'].unique():
        count = len(final_concepts[final_concepts['vocabulary_id'] == vocab])
        logger.info(f"  {vocab}: {count:,}")
    
    logger.info("By domain:")
    for domain in final_concepts['domain_id'].value_counts().head(10).items():
        logger.info(f"  {domain[0]}: {domain[1]:,}")
    
    # Step 8: Build Name nodes
    logger.info("Step 8: Building Name nodes...")
    names_list = []
    
    # Get Chinese language ID
    chinese_lang_id = [lid for lid, lname in language_map.items() if lname == 'Chinese'][0]
    
    # Add Chinese names from Excel for ICD-10
    for _, concept in final_concepts.iterrows():
        if concept['vocabulary_id'] == 'ICD10CM' and concept['concept_code'] in icd10cm_chinese:
            names_list.append({
                'concept_id': concept['concept_id'],
                'value': icd10cm_chinese[concept['concept_code']],
                'language_concept_id': chinese_lang_id,
                'language_name': 'Chinese'
            })
        elif concept['vocabulary_id'] == 'ICD10PCS' and concept['concept_code'] in icd10pcs_chinese:
            names_list.append({
                'concept_id': concept['concept_id'],
                'value': icd10pcs_chinese[concept['concept_code']],
                'language_concept_id': chinese_lang_id,
                'language_name': 'Chinese'
            })
    
    logger.info(f"Added {len(names_list)} Chinese names from Excel")
    
    # Add names from CONCEPT_SYNONYM for ALL target languages
    excel_chinese_concepts = set([n['concept_id'] for n in names_list])
    
    excluded_skipped = 0
    
    for _, row in target_synonyms.iterrows():
        concept_id = row['concept_id']
        
        # Skip if concept not in our final set (after domain filters)
        if concept_id not in all_concept_ids:
            if concept_id in excluded_concept_ids:
                excluded_skipped += 1
            continue
        
        lang_id = row['language_concept_id']
        lang_name = language_map[lang_id]
        synonym_value = row['concept_synonym_name']
        
        # For Chinese: skip ICD-10 concepts (we use Excel), but include SNOMED
        if lang_name == 'Chinese':
            concept_vocab = final_concepts[final_concepts['concept_id'] == concept_id]['vocabulary_id'].iloc[0] if len(final_concepts[final_concepts['concept_id'] == concept_id]) > 0 else None
            if concept_vocab in ['ICD10CM', 'ICD10PCS']:
                continue  # Skip, we have it from Excel
        
        names_list.append({
            'concept_id': concept_id,
            'value': synonym_value,
            'language_concept_id': lang_id,
            'language_name': lang_name
        })
    
    logger.info(f"Skipped {excluded_skipped:,} name entries for excluded domain concepts")
    logger.info(f"Total names before deduplication: {len(names_list):,}")
    
    # Create DataFrame and remove duplicates
    names_df = pd.DataFrame(names_list)
    names_df = names_df.drop_duplicates(subset=['concept_id', 'language_concept_id', 'value'])
    
    logger.info(f"Final names after deduplication: {len(names_df):,}")
    logger.info("Name distribution by language:")
    for lang_name in sorted(names_df['language_name'].unique()):
        count = len(names_df[names_df['language_name'] == lang_name])
        logger.info(f"  {lang_name}: {count:,}")
    
    # Step 9: Filter relationships
    logger.info("Step 9: Filtering relationships...")
    
    final_concept_ids = set(final_concepts['concept_id'].tolist())
    
    # IS_A relationships
    is_a_rels = relationships_df[
        (relationships_df['relationship_id'] == 'Is a') &
        (relationships_df['concept_id_1'].isin(final_concept_ids)) &
        (relationships_df['concept_id_2'].isin(final_concept_ids))
    ][['concept_id_1', 'concept_id_2']].copy()
    
    # MAPS_TO relationships (no self-maps)
    maps_to_rels = relationships_df[
        (relationships_df['relationship_id'] == 'Maps to') &
        (relationships_df['concept_id_1'].isin(final_concept_ids)) &
        (relationships_df['concept_id_2'].isin(final_concept_ids)) &
        (relationships_df['concept_id_1'] != relationships_df['concept_id_2'])
    ][['concept_id_1', 'concept_id_2']].copy()
    
    logger.info(f"IS_A relationships: {len(is_a_rels):,}")
    logger.info(f"MAPS_TO relationships: {len(maps_to_rels):,}")
    
    # Step 10: Build Neo4j graph
    logger.info("Step 10: Building Neo4j graph...")
    
    builder = MultilingualNeo4jGraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        builder.clear_database()
        builder.create_constraints()
        builder.create_indexes()
        builder.batch_create_concepts(final_concepts)
        builder.batch_create_names(names_df)
        builder.batch_create_relationships(is_a_rels, "IS_A")
        builder.batch_create_relationships(maps_to_rels, "MAPS_TO")
        
        logger.info("="*60)
        logger.info("Graph building completed successfully!")
        logger.info("="*60)
        logger.info(f"Total Concept nodes: {len(final_concepts):,}")
        logger.info(f"Total Name nodes: {len(names_df):,}")
        logger.info(f"Total IS_A relationships: {len(is_a_rels):,}")
        logger.info(f"Total MAPS_TO relationships: {len(maps_to_rels):,}")
        logger.info("="*60)
        
    finally:
        builder.close()


if __name__ == "__main__":
    build_graph(DATA_DIR)
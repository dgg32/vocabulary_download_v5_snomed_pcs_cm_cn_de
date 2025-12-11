#!/usr/bin/env python3
"""
Diagnostic script to check what languages are in your CONCEPT_SYNONYM file
"""

import pandas as pd
from pathlib import Path

DATA_DIR = "/Users/dgg32/Documents/claude/vocabulary_download_v5_snomed_pcs_cm_cn_de"

def check_languages():
    print("="*60)
    print("Checking language data in CONCEPT_SYNONYM...")
    print("="*60)
    
    # Load language mappings
    language_file = Path(DATA_DIR) / "language_id.csv"
    languages_df = pd.read_csv(language_file)
    language_map = dict(zip(languages_df['language_concept_id'], languages_df['language_name']))
    
    print("\nExpected languages from language_id.csv:")
    for lang_id, lang_name in language_map.items():
        print(f"  {lang_id}: {lang_name}")
    
    # Load CONCEPT_SYNONYM
    synonym_file = Path(DATA_DIR) / "CONCEPT_SYNONYM_cleaned.csv"
    print(f"\nLoading {synonym_file}...")
    synonyms_df = pd.read_csv(synonym_file, sep='\t')
    
    print(f"Total synonym entries: {len(synonyms_df):,}")
    
    # Check what languages are actually present
    print("\nLanguages actually in CONCEPT_SYNONYM:")
    lang_counts = synonyms_df['language_concept_id'].value_counts()
    
    for lang_id, count in lang_counts.items():
        lang_name = language_map.get(lang_id, f"Unknown({lang_id})")
        print(f"  {lang_id}: {lang_name} - {count:,} entries")
    
    # Check for expected languages
    print("\nChecking expected languages:")
    for expected_id, expected_name in language_map.items():
        if expected_id in lang_counts.index:
            print(f"  ✅ {expected_name} ({expected_id}): {lang_counts[expected_id]:,} entries")
        else:
            print(f"  ❌ {expected_name} ({expected_id}): NOT FOUND in CONCEPT_SYNONYM")
    
    # Sample some entries
    print("\nSample entries:")
    for lang_id in language_map.keys():
        samples = synonyms_df[synonyms_df['language_concept_id'] == lang_id].head(3)
        if len(samples) > 0:
            lang_name = language_map[lang_id]
            print(f"\n{lang_name} ({lang_id}):")
            for _, row in samples.iterrows():
                print(f"  Concept {row['concept_id']}: {row['concept_synonym_name']}")
    
    # Check if Japanese and German exist
    print("\n" + "="*60)
    print("DIAGNOSTIC SUMMARY:")
    print("="*60)
    
    japanese_id = 4181524
    german_id = 4182504
    
    if japanese_id in lang_counts.index:
        print(f"✅ Japanese data EXISTS: {lang_counts[japanese_id]:,} entries")
    else:
        print("❌ Japanese data MISSING from CONCEPT_SYNONYM_cleaned.csv")
        print("   → You need to add Japanese entries to the file")
    
    if german_id in lang_counts.index:
        print(f"✅ German data EXISTS: {lang_counts[german_id]:,} entries")
    else:
        print("❌ German data MISSING from CONCEPT_SYNONYM_cleaned.csv")
        print("   → You need to add German entries to the file")

if __name__ == "__main__":
    check_languages()
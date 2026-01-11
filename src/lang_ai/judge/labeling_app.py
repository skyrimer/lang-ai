"""
Streamlit application for manual labeling of disagreement samples between LLM Judges.
Used to create 'golden labels' for evaluation.
"""

import glob
import os

import pandas as pd
import streamlit as st

st.set_page_config(page_title="LLM Judge Labeling Tool", layout="wide")

st.title("🔍 LLM Judge Disagreement Labeler")

# Configuration
RESULTS_DIR = st.sidebar.text_input("Results Directory", "results")
dataset_files = glob.glob(os.path.join(RESULTS_DIR, "disagreement_samples_*.csv"))
datasets = [
    os.path.basename(f).replace("disagreement_samples_", "").replace(".csv", "")
    for f in dataset_files
]

if not datasets:
    st.warning(f"No disagreement samples found in {RESULTS_DIR}")
    st.stop()

selected_dataset = st.sidebar.selectbox("Select Dataset", datasets)

# Load data
csv_path = os.path.join(RESULTS_DIR, f"disagreement_samples_{selected_dataset}.csv")
df = pd.read_csv(csv_path)

# Prepare Golden Labels file path
golden_labels_path = os.path.join(RESULTS_DIR, f"golden_labels_{selected_dataset}.csv")

# Initialize or load golden labels
if os.path.exists(golden_labels_path):
    golden_df = pd.read_csv(golden_labels_path)
    # Merge existing labels
    df = df.merge(golden_df[["post", "manual_label"]], on="post", how="left")
    # Use existing labels to update the dataframe
else:
    df["manual_label"] = None

st.write(f"Showing {len(df)} samples with highest disagreement.")

# Labeling Interface
if "current_index" not in st.session_state:
    st.session_state.current_index = 0

# If we just loaded the data, make sure df matches session state if possible,
# but streamlit runs top to bottom, so it's easier to just use df.at[idx, 'manual_label'] = ...
# and save the whole thing.


def save_labels():
    labels_to_save = df[["post", "manual_label"]].dropna()
    labels_to_save.to_csv(golden_labels_path, index=False)
    st.sidebar.success(f"Saved {len(labels_to_save)} labels to {golden_labels_path}")


# Display current sample
idx = st.session_state.current_index
row = df.iloc[idx]

st.markdown(f"### Sample {idx + 1} / {len(df)}")
st.info(f"**Post:**\n\n{row['post']}")

col1, col2, col3 = st.columns(3)
with col1:
    st.write("**Estimated Truth (Dawid-Skene):**", row.get("estimated_truth", "N/A"))
with col2:
    st.write("**Vote Split (LEAKY/SAFE):**", row.get("vote_split", "N/A"))
with col3:
    st.write("**Disagreement Score:**", row.get("disagreement_score", "N/A"))

st.write("---")
st.write("**Judge Details:**")
judge_cols = [
    c
    for c in df.columns
    if c
    not in [
        "post",
        "estimated_truth",
        "vote_split",
        "disagreement_score",
        "manual_label",
    ]
]
for jc in judge_cols:
    st.write(f"- **{jc}**: {row[jc]}")

st.write("---")

# Labeling buttons
c1, c2, c3, c4 = st.columns(4)

with c1:
    if st.button("🚨 LEAKY", use_container_width=True):
        df.at[idx, "manual_label"] = "LEAKY"
        save_labels()
        if st.session_state.current_index < len(df) - 1:
            st.session_state.current_index += 1
            st.rerun()

with c2:
    if st.button("✅ SAFE", use_container_width=True):
        df.at[idx, "manual_label"] = "SAFE"
        save_labels()
        if st.session_state.current_index < len(df) - 1:
            st.session_state.current_index += 1
            st.rerun()

with c3:
    if st.button("Previous", disabled=(idx == 0), use_container_width=True):
        st.session_state.current_index -= 1
        st.rerun()

with c4:
    if st.button("Next", disabled=(idx == len(df) - 1), use_container_width=True):
        st.session_state.current_index += 1
        st.rerun()

# Status summary
st.write("---")
labeled_count = df["manual_label"].notnull().sum()
st.write(f"Progress: {labeled_count} / {len(df)} labeled")

if st.button("Force Save All"):
    save_labels()

# Show dataframe
with st.expander("Show all samples"):
    st.dataframe(df)

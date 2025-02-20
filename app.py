import streamlit as st
import pandas as pd
import plotly.express as px
import io
import time
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from PIL import Image

# --------------------
# 1) Config / Constants
# --------------------
LABEL_COLUMNS = ["Junk", "LowQuality", "Normal", "Stricture", "Ulcer"]

# If you're storing IDs in secrets:
# Example usage: st.secrets["gdrive"]["frames_folder_id"]
FRAMES_FOLDER_ID = "1u3epzHBLhyY9LU1ji9TqJl8xhTLJkxr-"
FRAMES_DS_FILE_ID = "1V7pO4u34IqF8Xx1Dc0gPRFmsGWZB7REV"
UNLABELED_FILE_ID = None  # set to a known file ID if unlabeled.csv already exists

# --------------------
# 2) Google Drive Authentication
# --------------------
def init_drive_local():
    """
    Basic PyDrive local webserver auth.
    Works locally, but can be problematic on Streamlit Cloud.
    Consider using a service account for deployment.
    """
    gauth = GoogleAuth()
    # This references client_secret.json in your local folder,
    # or you can store client secret data in st.secrets.
    gauth.LoadCredentialsFile("credentials.txt")
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile("credentials.txt")
    elif gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile("credentials.txt")
    else:
        gauth.Authorize()
    return GoogleDrive(gauth)

@st.cache_data
def load_csv_from_drive(drive, file_id):
    file = drive.CreateFile({'id': file_id})
    file.FetchMetadata()
    content = file.GetContentString()
    df = pd.read_csv(io.StringIO(content))
    return df

def save_df_to_drive(drive, df, file_id):
    if not file_id:
        st.warning("No file ID specified for saving. Skipping.")
        return
    file = drive.CreateFile({'id': file_id})
    csv_string = df.to_csv(index=False)
    file.SetContentString(csv_string)
    file.Upload()

def get_frame_files_in_folder(drive, folder_id):
    """Return list of (filename, file_id) for images in the given folder."""
    query = f"'{folder_id}' in parents and mimeType contains 'image/'"
    file_list = drive.ListFile({'q': query}).GetList()
    return [(f['title'], f['id']) for f in file_list]

def sync_unlabeled(df_frames, df_unlabeled, all_frame_files):
    """Add any new frames from the folder to the unlabeled df."""
    if df_frames.empty:
        existing_frames = set()
    else:
        existing_frames = set(df_frames['frame'].values)

    if df_unlabeled.empty:
        unlabeled_frames = set()
    else:
        unlabeled_frames = set(df_unlabeled['frame'].values)

    new_records = []
    for (fname, _) in all_frame_files:
        if fname not in existing_frames and fname not in unlabeled_frames:
            new_records.append({'frame': fname})

    if new_records:
        df_unlabeled = pd.concat([df_unlabeled, pd.DataFrame(new_records)], ignore_index=True)
    return df_unlabeled

# --------------------
# 3) Sidebar Filters
# --------------------
def sidebar_filters(df_frames, df_unlabeled):
    st.sidebar.header("Filters")
    label_status = st.sidebar.radio("Label Status", ["All", "Labeled", "Unlabeled"])

    # Collect possible movie/pillcam from both dataframes
    combined = pd.concat([df_frames, df_unlabeled], ignore_index=True)
    possible_movies = sorted(list(combined['movie'].dropna().unique()))
    movie_filter = st.sidebar.selectbox("Movie", ["All"] + possible_movies)

    possible_pillcams = sorted(list(combined['pillcam'].dropna().unique()))
    pillcam_filter = st.sidebar.selectbox("Pillcam", ["All"] + possible_pillcams)

    # Filter by label columns
    selected_label_filter = st.sidebar.multiselect("Has label(s)? (Optional)", LABEL_COLUMNS)

    return label_status, movie_filter, pillcam_filter, selected_label_filter

def apply_filters(df_frames, df_unlabeled, label_status, movie_filter, pillcam_filter, selected_label_filter):
    df_frames = df_frames.copy()
    df_unlabeled = df_unlabeled.copy()
    df_frames['is_labeled'] = True
    df_unlabeled['is_labeled'] = False

    if label_status == "All":
        df_display = pd.concat([df_frames, df_unlabeled], ignore_index=True)
    elif label_status == "Labeled":
        df_display = df_frames
    else:  # "Unlabeled"
        df_display = df_unlabeled

    if movie_filter != "All":
        df_display = df_display[df_display['movie'] == movie_filter]

    if pillcam_filter != "All":
        df_display = df_display[df_display['pillcam'] == pillcam_filter]

    if selected_label_filter:
        for lab_col in selected_label_filter:
            if lab_col in df_display.columns:
                df_display = df_display[df_display[lab_col] == 1]

    return df_display

# --------------------
# 4) Navigation & Display
# --------------------
def show_navigation(df_display):
    st.write(f"Found {len(df_display)} frame(s) after filtering.")

    if len(df_display) == 0:
        return None

    if "current_index" not in st.session_state:
        st.session_state.current_index = 0

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        if st.button("Previous"):
            st.session_state.current_index = max(st.session_state.current_index - 1, 0)
    with col3:
        if st.button("Next"):
            st.session_state.current_index = min(st.session_state.current_index + 1, len(df_display) - 1)

    idx = st.session_state.current_index
    row = df_display.iloc[idx]
    return row

def load_image_from_drive(drive, file_id):
    file = drive.CreateFile({'id': file_id})
    content = file.GetContentBinary()
    return Image.open(io.BytesIO(content))

def label_frame_ui(row, drive, frames_folder_files):
    st.subheader(f"Frame: {row['frame']}")
    file_id = None
    for (fname, f_id) in frames_folder_files:
        if fname == row['frame']:
            file_id = f_id
            break

    if file_id:
        # Lazy load the image
        img = load_image_from_drive(drive, file_id)
        st.image(img, use_column_width=True)
    else:
        st.error("Could not find image in Drive folder.")

    if "temp_labels" not in st.session_state:
        st.session_state["temp_labels"] = {}

    frame_key = row['frame']

    # Fetch existing or default label states
    if frame_key in st.session_state["temp_labels"]:
        current_labels = st.session_state["temp_labels"][frame_key]
    else:
        current_labels = {}
        for lab in LABEL_COLUMNS:
            current_labels[lab] = int(row.get(lab, 0) if lab in row else 0)

    updated_labels = {}
    for lab in LABEL_COLUMNS:
        ck = st.checkbox(lab, value=(current_labels[lab] == 1))
        updated_labels[lab] = 1 if ck else 0

    # Store updates
    st.session_state["temp_labels"][frame_key] = updated_labels

# --------------------
# 5) Updating CSV
# --------------------
def update_csvs(drive, df_frames, df_unlabeled):
    if "temp_labels" not in st.session_state or not st.session_state["temp_labels"]:
        st.info("No changes to save.")
        return df_frames, df_unlabeled

    changed_count = 0

    for frame_key, labels_dict in st.session_state["temp_labels"].items():
        # If the frame is in df_frames
        if frame_key in df_frames['frame'].values:
            idx = df_frames.index[df_frames['frame'] == frame_key][0]

            # Update each label column
            for lab in LABEL_COLUMNS:
                df_frames.at[idx, lab] = labels_dict[lab]

            # Build a "class" column from these labels
            assigned = [k for k,v in labels_dict.items() if v == 1]
            if len(assigned) == 1:
                df_frames.at[idx, 'class'] = assigned[0]
            elif len(assigned) > 1:
                df_frames.at[idx, 'class'] = ",".join(assigned)
            else:
                df_frames.at[idx, 'class'] = ""

            df_frames.at[idx, 'label_date'] = time.strftime('%Y-%m-%d %H:%M:%S')
            changed_count += 1

        else:
            # Then it's presumably in df_unlabeled
            # We'll move it to df_frames if we assigned any labels
            new_row = {
                'frame': frame_key,
                'movie': "",
                'pillcam': "",
                'label_date': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            for lab in LABEL_COLUMNS:
                new_row[lab] = labels_dict[lab]

            assigned = [k for k,v in labels_dict.items() if v == 1]
            if assigned:
                new_row['class'] = ",".join(assigned)
            else:
                new_row['class'] = ""

            df_frames = pd.concat([df_frames, pd.DataFrame([new_row])], ignore_index=True)
            df_unlabeled = df_unlabeled[df_unlabeled['frame'] != frame_key]
            changed_count += 1

    # Clear the temp labels
    st.session_state["temp_labels"] = {}
    st.success(f"Updated labels for {changed_count} frame(s).")

    # Save changes back to Drive
    save_df_to_drive(drive, df_frames, FRAMES_DS_FILE_ID)
    save_df_to_drive(drive, df_unlabeled, UNLABELED_FILE_ID)

    return df_frames, df_unlabeled

# --------------------
# 6) Visualizations
# --------------------
def show_label_distribution(df_frames):
    counts = {}
    for lab in LABEL_COLUMNS:
        if lab in df_frames.columns:
            counts[lab] = df_frames[lab].sum()
        else:
            counts[lab] = 0
    df_counts = pd.DataFrame({"label": list(counts.keys()), "count": list(counts.values())})

    fig = px.pie(df_counts, names="label", values="count", title="Label Distribution (Multi-label)")
    st.plotly_chart(fig, use_container_width=True)

def show_labeled_vs_unlabeled(df_frames, df_unlabeled):
    data = pd.DataFrame({
        'status': ["Labeled", "Unlabeled"],
        'count': [len(df_frames), len(df_unlabeled)]
    })
    fig = px.pie(data, names="status", values="count", title="Labeled vs Unlabeled")
    st.plotly_chart(fig, use_container_width=True)

def show_visualizations(df_frames, df_unlabeled):
    st.subheader("Visualizations")
    tab1, tab2 = st.tabs(["Label Distribution", "Labeled vs Unlabeled"])
    with tab1:
        show_label_distribution(df_frames)
    with tab2:
        show_labeled_vs_unlabeled(df_frames, df_unlabeled)

# --------------------
# Main App
# --------------------
def main():
    st.title("Capsule Endoscopy Labeling App")

    # Initialize PyDrive
    drive = init_drive_local()

    # Load frames_ds.csv
    try:
        df_frames = load_csv_from_drive(drive, FRAMES_DS_FILE_ID)
    except:
        st.warning("No frames_ds.csv found or invalid FILE ID. Creating empty.")
        df_frames = pd.DataFrame(columns=["frame","class","movie","pillcam","label_date"] + LABEL_COLUMNS)

    # Load unlabeled.csv if it exists
    if UNLABELED_FILE_ID:
        try:
            df_unlabeled = load_csv_from_drive(drive, UNLABELED_FILE_ID)
        except:
            st.warning("Could not load unlabeled.csv. Creating empty.")
            df_unlabeled = pd.DataFrame(columns=df_frames.columns)
    else:
        # No unlabeled ID - create empty
        df_unlabeled = pd.DataFrame(columns=df_frames.columns)

    # Ensure columns exist
    for col in ["frame","class","movie","pillcam","label_date"] + LABEL_COLUMNS:
        if col not in df_frames.columns:
            df_frames[col] = None
        if col not in df_unlabeled.columns:
            df_unlabeled[col] = None

    # Sync new frames
    frames_folder_files = get_frame_files_in_folder(drive, FRAMES_FOLDER_ID)
    df_unlabeled = sync_unlabeled(df_frames, df_unlabeled, frames_folder_files)

    # Sidebar filters
    label_status, movie_filter, pillcam_filter, selected_label_filter = sidebar_filters(df_frames, df_unlabeled)
    df_display = apply_filters(df_frames, df_unlabeled, label_status, movie_filter, pillcam_filter, selected_label_filter)

    # Show navigation
    row = show_navigation(df_display)
    if row is not None:
        label_frame_ui(row, drive, frames_folder_files)

    # Update CSV button
    if st.button("Update CSV"):
        df_frames, df_unlabeled = update_csvs(drive, df_frames, df_unlabeled)

    st.divider()
    # Show some charts
    show_visualizations(df_frames, df_unlabeled)

if __name__ == "__main__":
    main()

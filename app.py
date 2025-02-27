import streamlit as st
import pandas as pd
import plotly.express as px
import io
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from PIL import Image
import openpyxl

# ------------ CONFIG ------------
LABEL_COLUMNS = ["Junk", "LowQuality", "Normal", "Stricture", "Ulcer"]

# ------------ SERVICE ACCOUNT AUTH ------------
@st.cache_resource
def init_drive_service():
    """
    Build and return a Google Drive service instance using service account info.
    """
    sa_info = st.secrets["gdrive_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build('drive', 'v3', credentials=creds)
    return service

# ------------ DRIVE HELPER FUNCTIONS ------------
def download_excel_from_drive(drive_service, file_id) -> pd.DataFrame:
    """
    Download an Excel file from Drive using its file_id and load it into a DataFrame.
    """
    if not file_id:
        return pd.DataFrame()

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    df = pd.read_excel(fh, engine='openpyxl')
    return df

def upload_excel_to_drive(drive_service, df: pd.DataFrame, file_id: str):
    """
    Overwrite an Excel file on Drive with the DataFrame content.
    """
    if not file_id:
        st.warning("No file ID specified for uploading. Skipping.")
        return

    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    media_body = MediaIoBaseUpload(
        excel_buffer,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    update_request = drive_service.files().update(
        fileId=file_id,
        media_body=media_body
    )
    update_request.execute()

def list_frames_in_folder(drive_service, folder_id: str):
    """
    List image files (file_id and file_name) in a given Drive folder.
    """
    files_list = []
    page_token = None
    query = f"'{folder_id}' in parents and (mimeType contains 'image/')"
    while True:
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token
        ).execute()
        for f in response.get('files', []):
            files_list.append((f['id'], f['name']))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return files_list

def download_image(drive_service, file_id):
    """
    Download an image from Drive by file_id and return a PIL Image.
    """
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return Image.open(fh)

# ------------ CACHED VERSIONS ------------
@st.cache_data(ttl=3600)
def cached_download_excel(file_id: str) -> pd.DataFrame:
    drive_service = init_drive_service()
    return download_excel_from_drive(drive_service, file_id)

@st.cache_data(ttl=3600)
def cached_list_frames(folder_id: str):
    drive_service = init_drive_service()
    return list_frames_in_folder(drive_service, folder_id)

@st.cache_data(show_spinner=False)
def cached_download_image(file_id: str):
    drive_service = init_drive_service()
    return download_image(drive_service, file_id)

# ------------ Excel / LABELING LOGIC ------------
def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    base_cols = ["frame", "class", "movie", "pillcam", "label_date"]
    for col in base_cols + LABEL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df

def sync_unlabeled(df_frames, df_unlabeled, all_frame_files):
    existing_frames = set(df_frames['frame'].dropna().unique())
    unlabeled_frames = set(df_unlabeled['frame'].dropna().unique())
    new_records = []
    for (file_id, file_name) in all_frame_files:
        if file_name not in existing_frames and file_name not in unlabeled_frames:
            new_records.append({"frame": file_name})
    if new_records:
        df_unlabeled = pd.concat([df_unlabeled, pd.DataFrame(new_records)], ignore_index=True)
    return df_unlabeled

def merge_temp_labels(df_frames, df_unlabeled):
    temp_labels = st.session_state.get("temp_labels", {})
    if not temp_labels:
        st.info("No changes to save.")
        return df_frames, df_unlabeled, 0

    changed_count = 0
    for frame_name, label_dict in temp_labels.items():
        if frame_name in df_frames['frame'].values:
            idx = df_frames.index[df_frames['frame'] == frame_name][0]
            for lab_col in LABEL_COLUMNS:
                df_frames.at[idx, lab_col] = label_dict.get(lab_col, 0)
            assigned = [k for k, v in label_dict.items() if v == 1]
            df_frames.at[idx, 'class'] = assigned[0] if len(assigned) == 1 else ",".join(assigned) if assigned else ""
            df_frames.at[idx, 'label_date'] = time.strftime('%Y-%m-%d %H:%M:%S')
            changed_count += 1
        else:
            new_row = {
                'frame': frame_name,
                'movie': "",
                'pillcam': "",
                'label_date': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            for lab_col in LABEL_COLUMNS:
                new_row[lab_col] = label_dict.get(lab_col, 0)
            assigned = [k for k, v in label_dict.items() if v == 1]
            new_row['class'] = ",".join(assigned) if assigned else ""
            df_frames = pd.concat([df_frames, pd.DataFrame([new_row])], ignore_index=True)
            df_unlabeled = df_unlabeled[df_unlabeled['frame'] != frame_name]
            changed_count += 1

    st.session_state["temp_labels"] = {}
    return df_frames, df_unlabeled, changed_count

# ------------ STREAMLIT UI FUNCTIONS ------------
def sidebar_filters(df_frames, df_unlabeled):
    st.sidebar.header("Filters")
    status = st.sidebar.radio("Show Which?", ["All", "Labeled", "Unlabeled"])
    combined = pd.concat([df_frames, df_unlabeled], ignore_index=True)
    movies = sorted(list(combined['movie'].dropna().unique()))
    movie_filter = st.sidebar.selectbox("Movie", ["All"] + movies)
    pillcams = sorted(list(combined['pillcam'].dropna().unique()))
    pillcam_filter = st.sidebar.selectbox("Pillcam", ["All"] + pillcams)
    label_sel = st.sidebar.multiselect("Has Label(s)?", LABEL_COLUMNS)
    return status, movie_filter, pillcam_filter, label_sel

def apply_filters(df_frames, df_unlabeled, status, movie_filter, pillcam_filter, label_sel):
    df_frames = df_frames.copy()
    df_frames["is_labeled"] = True
    df_unlabeled = df_unlabeled.copy()
    df_unlabeled["is_labeled"] = False

    if status == "All":
        df_show = pd.concat([df_frames, df_unlabeled], ignore_index=True)
    elif status == "Labeled":
        df_show = df_frames
    else:
        df_show = df_unlabeled

    if movie_filter != "All":
        df_show = df_show[df_show['movie'] == movie_filter]
    if pillcam_filter != "All":
        df_show = df_show[df_show['pillcam'] == pillcam_filter]
    if label_sel:
        for lab in label_sel:
            if lab in df_show.columns:
                df_show = df_show[df_show[lab] == 1]
    return df_show

def navigation(df):
    st.write(f"Found {len(df)} frame(s) after filtering.")
    if len(df) == 0:
        return None
    if "current_index" not in st.session_state:
        st.session_state.current_index = 0

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        if st.button("Previous"):
            st.session_state.current_index = max(st.session_state.current_index - 1, 0)
    with col3:
        if st.button("Next"):
            st.session_state.current_index = min(st.session_state.current_index + 1, len(df) - 1)

    idx = st.session_state.current_index
    row = df.iloc[idx]
    return row

def display_frame(row, all_files):
    st.subheader(f"Frame: {row['frame']}")
    file_id = None
    for (fid, fname) in all_files:
        if fname == row['frame']:
            file_id = fid
            break

    if file_id:
        # Use the cached image download function
        img = cached_download_image(file_id)
        st.image(img, use_container_width=True)
    else:
        st.error("Image not found in Drive folder. Possibly it's missing?")

def labeling_ui(row):
    if "temp_labels" not in st.session_state:
        st.session_state["temp_labels"] = {}
    frame_name = row['frame']
    current_dict = st.session_state["temp_labels"].get(frame_name, {})
    if not current_dict:
        current_dict = {}
        for lab in LABEL_COLUMNS:
            val = row.get(lab, 0)
            current_dict[lab] = 1 if val == 1 else 0

    updated_dict = {}
    for lab in LABEL_COLUMNS:
        ck = st.checkbox(lab, value=(current_dict[lab] == 1), key=f"{frame_name}_{lab}")
        updated_dict[lab] = 1 if ck else 0

    st.session_state["temp_labels"][frame_name] = updated_dict

def show_visualizations(df_frames, df_unlabeled):
    st.subheader("Visualizations")
    tab1, tab2 = st.tabs(["Label Distribution", "Labeled vs Unlabeled"])
    with tab1:
        counts = {lab: df_frames[lab].sum() if lab in df_frames.columns else 0 for lab in LABEL_COLUMNS}
        dist_df = pd.DataFrame({"label": list(counts.keys()), "count": list(counts.values())})
        fig1 = px.pie(dist_df, names="label", values="count", title="Label Distribution")
        st.plotly_chart(fig1, use_container_width=True)
    with tab2:
        labeled_count = len(df_frames)
        unlabeled_count = len(df_unlabeled)
        data = pd.DataFrame({
            'status': ['Labeled', 'Unlabeled'],
            'count': [labeled_count, unlabeled_count]
        })
        fig2 = px.pie(data, names='status', values='count', title="Labeled vs Unlabeled")
        st.plotly_chart(fig2, use_container_width=True)

def show_usage_description():
    st.markdown(
        """
        ## Usage & Features

        **Features**  
        1. **Multi-label classification** for endoscopy frames (Junk, LowQuality, Normal, Stricture, Ulcer).  
        2. **Automatic detection of new (unlabeled) frames** in the Drive folder.  
        3. **Filtering** by Movie, Pillcam, or any label.  
        4. **Visualizations** (Pie Charts for Label Distribution and Labeled vs. Unlabeled).  
        5. **Saves changes** back to Excel on Google Drive.

        ---  
        **How to Use**  
        1. **Navigate** through frames using the "Previous" and "Next" buttons.  
        2. **Toggle** checkboxes to assign or remove labels for each frame.  
        3. Click **"Update Excel"** to commit your label changes (the updated file overwrites the original Excel file on Drive).  
        4. Check the **Visualizations** tabs to see distribution of labels and labeled/unlabeled stats.
        """
    )

# ------------ MAIN APP ------------
def main():
    st.title("Capsule Endoscopy Labeling App")
    drive_service = init_drive_service()
    folder_id = st.secrets["gdrive"]["frames_folder_id"]
    frames_ds_file_id = st.secrets["gdrive"]["frames_ds_file_id"]
    unlabeled_file_id = st.secrets["gdrive"].get("unlabeled_file_id", None)

    # Load Excel files via cached functions
    df_frames = cached_download_excel(frames_ds_file_id)
    df_frames = ensure_columns(df_frames)
    df_unlabeled = cached_download_excel(unlabeled_file_id)
    df_unlabeled = ensure_columns(df_unlabeled)

    # List all frame files using cache
    all_files = cached_list_frames(folder_id)
    df_unlabeled = sync_unlabeled(df_frames, df_unlabeled, all_files)

    # Sidebar filters and apply filtering
    status, movie_filter, pillcam_filter, label_sel = sidebar_filters(df_frames, df_unlabeled)
    df_display = apply_filters(df_frames, df_unlabeled, status, movie_filter, pillcam_filter, label_sel)

    row = navigation(df_display)
    if row is not None:
        display_frame(row, all_files)
        labeling_ui(row)

    st.divider()

    # Update Excel on Drive and clear caches to load fresh data next time
    if st.button("Update Excel"):
        df_frames, df_unlabeled, changed_count = merge_temp_labels(df_frames, df_unlabeled)
        if changed_count > 0:
            upload_excel_to_drive(drive_service, df_frames, frames_ds_file_id)
            if unlabeled_file_id:
                upload_excel_to_drive(drive_service, df_unlabeled, unlabeled_file_id)
            st.success(f"Updated {changed_count} frame(s).")
            st.cache_data.clear()  # Invalidate cache after updates
        else:
            st.info("No changes to commit.")

    show_visualizations(df_frames, df_unlabeled)
    show_usage_description()

if __name__ == "__main__":
    main()

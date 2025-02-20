# Capsule Endoscopy Labeler

This repository contains a Streamlit app for labeling endoscopic frame images stored on Google Drive. 

## Features
- Multi-label classification for frames: `Junk`, `LowQuality`, `Normal`, `Stricture`, `Ulcer`
- Checks for new frames in Google Drive and appends them to `unlabeled.csv`
- Filters by `movie`, `pillcam`, labeled/unlabeled, etc.
- Pie charts to visualize label distribution and labeled/unlabeled stats
- Stores changes in a temporary state until the user clicks **Update CSV** to commit changes to `frames_ds.csv`

## Installation

1. Install [Anaconda](https://www.anaconda.com/) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html).
2. Clone this repo:
   ```bash
   git clone https://github.com/your-username/capsule_endoscopy_labeler.git
   cd capsule_endoscopy_labeler

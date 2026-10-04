#!/bin/sh
# Phát lặp video mẫu vào MediaMTX qua RTSP. Chèn giờ thực lên hình để giả lập OSD camera (ADR-008).
# Biến: CAM (cam1|cam2), TARGET (rtsp://mediamtx:8554/cam-fake1).
set -eu

FONT=/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf
OSD="drawtext=fontfile=${FONT}:text='%{localtime\\:%Y-%m-%d %H\\\\\\:%M\\\\\\:%S}':x=w-tw-30:y=30:fontsize=28:fontcolor=white:box=1:boxcolor=black@0.6"

until wget -q -O /dev/null "http://mediamtx:9997/v3/paths/list"; do
  echo "waiting for mediamtx..."; sleep 1
done

exec ffmpeg -hide_banner -loglevel warning -re -stream_loop -1 -i "/media/${CAM}.mp4" \
  -vf "${OSD}" -c:v libx264 -preset ultrafast -tune zerolatency -g 30 -pix_fmt yuv420p \
  -f rtsp -rtsp_transport tcp "${TARGET}"

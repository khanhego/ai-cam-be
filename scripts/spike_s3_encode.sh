#!/bin/sh
# Spike S3 phần 2 (T-5): so phương án encode SIDE_BY_SIDE cho AC-08 trên clip 180 giây từ camera giả.
# Chạy trong image aicam-dev (cùng mạng compose với mediamtx):
#   docker cp scripts/spike_s3_encode.sh aicam-dev-worker-1:/tmp/ && docker exec aicam-dev-worker-1 sh /tmp/spike_s3_encode.sh
# File tạm nằm trong thư mục tạm riêng, xóa khi xong. Dung lượng ≈ 60 MB.
set -eu
W=$(mktemp -d /tmp/spike-enc-XXXXXX)
trap 'rm -rf "$W"' EXIT
FONT=/usr/share/fonts/truetype/bevietnampro/BeVietnamPro-SemiBold.ttf
D=180

# Ghi 180 giây từ relay MediaMTX bằng stream copy (giống clip gốc), song song 2 camera.
ffmpeg -hide_banner -loglevel error -rtsp_transport tcp -i rtsp://mediamtx:8554/cam-fake1 -t $D -c copy "$W/c1.mp4" &
ffmpeg -hide_banner -loglevel error -rtsp_transport tcp -i rtsp://mediamtx:8554/cam-fake2 -t $D -c copy "$W/c2.mp4" &
wait

overlay="drawtext=fontfile=$FONT:text='SPXTST0000001 · 2410TST00001 · TST Station 01':x=24:y=24:fontsize=30:fontcolor=white:box=1:boxcolor=black@0.55,drawtext=fontfile=$FONT:text='%{pts\\:gmtime\\:1790000000\\:%d/%m/%Y %H\\\\\\:%M\\\\\\:%S}':x=24:y=72:fontsize=30:fontcolor=white:box=1:boxcolor=black@0.55"

run() { # tên, kích thước mỗi camera, preset, crf
  name=$1; size=$2; preset=$3; crf=$4
  sc="scale=$size:force_original_aspect_ratio=decrease,pad=$size:(ow-iw)/2:(oh-ih)/2,setsar=1"
  for i in 1 2 3; do
    s=$(date +%s.%N)
    ffmpeg -hide_banner -loglevel error -nostdin -y -i "$W/c1.mp4" -i "$W/c2.mp4" \
      -filter_complex "[0:v]$sc[a];[1:v]$sc[b];[a][b]hstack=inputs=2,$overlay[v]" -map "[v]" \
      -c:v libx264 -preset "$preset" -crf "$crf" -pix_fmt yuv420p -movflags +faststart -t $D "$W/o.mp4"
    e=$(date +%s.%N)
    echo "$name run$i $(python3 -c "print(round($e - $s, 2))") s $(stat -c %s "$W/o.mp4") bytes"
  done
}
echo "nproc=$(nproc) c1=$(stat -c %s "$W/c1.mp4") c2=$(stat -c %s "$W/c2.mp4")"
run sbs_1280x720_veryfast 1280:720 veryfast 26
run sbs_960x540_veryfast 960:540 veryfast 26
run sbs_1280x720_superfast 1280:720 superfast 26

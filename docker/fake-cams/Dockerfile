# Camera giả cho dev / test (03-plan T-2). Sinh video mẫu lúc build, phát RTSP lúc chạy.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core wget \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "python-barcode[images]==0.15.1" "pillow>=10"

COPY make_media.py publish.sh /opt/fake-cams/
RUN python /opt/fake-cams/make_media.py /media && rm -rf /media/work && chmod +x /opt/fake-cams/publish.sh

CMD ["/opt/fake-cams/publish.sh"]

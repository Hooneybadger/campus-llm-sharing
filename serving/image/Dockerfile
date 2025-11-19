# 서빙 이미지 = vLLM 공식 이미지 + 다중 프로세스 슈퍼바이저. 태그 규약: <vllm버전>-<슈퍼바이저 리비전>
FROM vllm/vllm-openai:v0.32.0
COPY serving/multi_vllm.py /opt/pknu/multi_vllm.py
ENTRYPOINT ["python3", "/opt/pknu/multi_vllm.py"]
CMD ["/etc/pknu/profile.yaml", "/etc/pknu/registry.yaml"]

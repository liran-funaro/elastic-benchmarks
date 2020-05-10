# Elastic Benchmarking


# Install (beta)
Install the package in develop mode:
```bash
python setup.py develop --user
```

Host requires to install `libvirt` and its Python API.

```bash
sudo apt install libvirt python-libvirt python3-libvirt
```

If you are using your own installation of Python, you might need to install `libvirt-python` manually via `pip`:
```bash
sudo apt install libvirt-dev
pip install libvirt-python
```

# License
[GPL](LICENSE.txt)

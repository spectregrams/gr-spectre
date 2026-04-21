# If we've already built the module, clean up.
if [ -d "build" ]; then
	cd build
	make uninstall
	cd ..
	rm -rf build
fi

# Rebuild the module.
mkdir build
cd build
cmake .. -DENABLE_TESTING=ON
make -j$(nproc)
make install
ldconfig
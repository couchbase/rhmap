default:
	go test ./...

generate-map_bytes_uint64:
	mkdir -p ./map_bytes_uint64
	cat rhmap.go | \
         sed -e 's/Package rhmap/Package map_bytes_uint64/g' | \
         sed -e 's/package rhmap/package map_bytes_uint64/g' | \
         sed -e 's/map\[\[\]byte\]\[\]byte/map\[\[\]byte\]uint64/g' | \
         sed -e 's/type Val \[\]byte/type Val uint64/g' | \
         sed -e 's/Val(nil)/Val(0)/g' | \
         cat > ./map_bytes_uint64/rhmap.go

generate-map_bytes_int:
	mkdir -p ./map_bytes_int
	cat rhmap.go | \
         sed -e 's/Package rhmap/Package map_bytes_int/g' | \
         sed -e 's/package rhmap/package map_bytes_int/g' | \
         sed -e 's/map\[\[\]byte\]\[\]byte/map\[\[\]byte\]int/g' | \
         sed -e 's/type Val \[\]byte/type Val int/g' | \
         sed -e 's/Val(nil)/Val(0)/g' | \
         cat > ./map_bytes_int/rhmap.go

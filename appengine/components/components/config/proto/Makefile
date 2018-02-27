compile:
	# If we simply compile *.proto files relative to this dir,
	# the recorded file names in the generated _pb2.py are relative to this dir
	# too, e.g. "project_config.proto". This prevents apps, such as buildbucket,
	# from using their own "project_conifg.proto".
	#
	# Move the root of proto compilation to //appengine/components.
	# This is consistent with other components assuming that "components"
	# is in the import path.
	cd ../../../ && protoc --python_out=. components/config/proto/*.proto

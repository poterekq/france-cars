import os
import re
import shutil
import sys
from dataclasses import dataclass
from typing import Iterable, Optional, Union
from urllib.request import urlretrieve

from py7zr import SevenZipFile
from zipfile import ZipFile


@dataclass
class Fmt:
	"""
	A class for storing string formatting attributes. This helps style
	text displayed in the standard output (`stdout`).
	"""
	GREEN = '\033[92m'
	RED = '\033[93m'
	BOLD = '\033[1m'
	END = '\033[0m'


@dataclass
class UrlManager:
	"""
	A class for storing URLs as f-string literals. Said URLs point
	to external spatial datasets.
	"""
	# {0}: release date
	ADMIN = (
		"https://wxs.ign.fr/"
		"x02uy2aiwjo9bm8ce5plwqmr/"
		"telechargement/prepackage/"
		"ADMINEXPRESS-COG-CARTO_SHP_WGS84G_PACK_{0}$"
		"ADMIN-EXPRESS-COG-CARTO_3-1__SHP__FRA_WM_{0}/"
		"file/"
		"ADMIN-EXPRESS-COG-CARTO_3-1__SHP__FRA_WM_{0}.7z"
	)

	CORINE = (
		"ftp://Corine_Land_Cover_ext:"
		"ishiteimapie9ahP@"
		"ftp3.ign.fr/"
		"CLC18_SHP__FRA_2019-08-21.7z"
	)

	# {0}: three-character long department INSEE number
	# {1}: release date
	BDTOPO = (
		"https://wxs.ign.fr/"
		"859x8t863h6a09o9o6fy4v60/"
		"telechargement/prepackage/" 
		"BDTOPOV3-TOUSTHEMES-DEPARTEMENT_GPKG_PACK_221$" 
		"BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D{0}_{1}/" 
		"file/" 
		"BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D{0}_{1}.7z"
	)

	# {0}: former French region name used by GeoFabrik
	OSM = (
		"http://download.geofabrik.de/"
		"europe/"
		"france/"
		"{0}-latest-free.shp.zip"
	)


@dataclass
class PatternManager:
	"""
	A class for storing regular expressions. They allow extracting
	specific files from the archives downloaded from the URLs contained
	in dataclass `UrlManager`.
	"""
	ADMIN = r".*[/]COMMUNE[.].*"
	CORINE = r".*[/]CLC18_FR[.].*"
	BDTOPO = r".*gpkg$"
	OSM = r".*traffic_a_free_1[.].*"


class FileManager:
	"""
	A class for managing data on the host computer. This includes
	downloading, listing, extracting and deleting files, folders and
	archives.
	"""
	def __init__(self, folder: Optional[str] = None) -> None:
		"""
		Initialize the file manager.

		TODO: Remove the `__init__()` method, since other methods were
		refactored to allow for directly passing them a string instead
		of relying on `self.folder`. 

		Parameters
		----------
		folder : str, optional
			Root folder where operations on files are to be executed.
		"""
		self.folder = folder
	

	@staticmethod
	def _report(
		count: int,
		block_size: int,
		total_size: int
	) -> None:
		"""
		Display information on the progress of a download. It it meant
		to be used with the `download_file()` method.

		Parameters
		----------
		count : int
			Chunk number.
		block_size : int
			Maximum size chunks are read in.
		total_size : int
			Total size of the download.
			
		Source
		------
		https://ofstack.com/python/12319/method-that-shows-the-download-progress-when-python-downloads-a-file.html
		"""
		percent = int(count * block_size * 100 / total_size)
		sys.stdout.write(f"\r[{str(percent).rjust(3, '·')}%]")
		sys.stdout.flush()


	def find_files(
		self,
		folder: str
	) -> Iterable[str]:
		"""
		Find all files in a parent folder and its children.

		Parameters
		----------
		folder : str
			Parent folder in which the files are searched.
		
		Returns
		-------
		iterable of str
			List of files found in the filesystem.
		"""
		files = []
		for dirpath, _, filenames in os.walk(folder):
			for filename in filenames:
				files.append(os.path.join(dirpath, filename))
		return files


	def find_match_files(
		self,
		src: Union[str, Iterable[str]],
		pattern: str,
	) -> Iterable[str]:
		"""
		Find files in a specific folder or list, that match the provided
		regular expression.

		Parameters
		----------
		src : str, iterable of str
			Either a folder or list in which to search for files. When 
			the search is carried out in a folder, it won't look for 
			files in sub-folders. When using a list, it should contain 
			absolute file paths.
		pattern : str
			Regular expression against which file names are matched.
		
		Returns
		-------
		iterable of str
			A list of file names that match the provided pattern.
		
		Raises
		------
		TypeError
			The data type of `src` is neither of type `str` or `list`. 
			Note: When using a list, it would be preferable to check 
			whether each item is a string or not...
		"""
		filter_pattern = re.compile(pattern)
		if isinstance(src, str):
			filtered = [f for f in os.listdir(src) if filter_pattern.match(f)]
		elif isinstance(src, list):
			filtered = [f for f in src if filter_pattern.match(f)]
		else:
			raise TypeError(f"This method does not accept {type(src)}s!")
		return filtered


	def delete(
		self, 
		path: str
	) -> None:
		"""
		Delete either a file or a folder.

		Parameters
		----------
		path : str
			Path of a file or folder.
		"""
		choice = input(f"{path} will be deleted. Are you sure [y/N]? ")

		if choice.lower() == "y":
			if os.path.isdir(path):
				shutil.rmtree(path, ignore_errors=True)
			elif os.path.isfile(path):
				os.remove(path)


	def split_path(
		self,
		path: str
	) -> Iterable[str]:
		"""
		Split a path with the os standard separator.

		Parameters
		----------
		path : str
			Path of a file or folder.
		
		Returns
		-------
		iterable of str
			List of items, each corresponding to a part of the full
			path.
		"""
		normalized_path = os.path.normpath(path)
		return normalized_path.split(os.sep)


	def download_file(
		self,
		src: str,
		folder: str,
		out_name: Optional[str] = None
	) -> str:
		"""
		Download a file from the provided source url.

		Parameters
		----------
		src : str
			URL from which to download a file.
		folder : str
			Folder in which to download a file.
		out_name : str, optional
			Name on the host system of the downloaded file. When
			`out_name` is not specified, the original name is kept.
						
		Returns
		-------
		str
			Absolute path on the host system of the downloaded file.
		"""
		src_name = os.path.basename(src)

		if out_name is None:
			out_path = os.path.join(folder, src_name)
		else:
			_, src_ext = os.path.splitext(src_name)
			_, dst_ext = os.path.splitext(out_name)
			if dst_ext == '':
				out_name += src_ext
			out_path = os.path.join(folder, out_name)

		print(f"{' ' * 7}Downloading {src_name}...", end="\r")
		urlretrieve(src, out_path, reporthook=self._report)
		print("\n")

		return out_path


	def extract_7z(
		self,
		src: str,
		dst: str,
		pattern: str,
	) -> Iterable[str]:
		"""
		Extract content from a 7z archive. Only content that match the
		provided regular expression is extracted.

		Parameters
		----------
		src : str
			Path to the 7z archive.
		dst : str
			Path where the archive content is to be extracted.
		pattern : str
			Regular expression against which to match archive content.
			Only matching content is extracted.
		
		Returns
		-------
		iterable of str
			List of absolute paths for the extracted content.
		"""
		with SevenZipFile(src, "r") as archive:
			files = archive.getnames()
			extracted = self.find_match_files(files, pattern)
			archive.extract(dst, extracted)

		return extracted


	def extract_zip(
		self,
		src: str,
		dst: str,
		pattern: str,
	) -> Iterable[str]:
		"""
		Extract content from a zip archive. Only content that match the
		provided regular expression is extracted.

		Parameters
		----------
		src : str
			Path to the zip archive.
		dst : str
			Path where the archive content is to be extracted.
		pattern : str
			Regular expression against which to match archive content. 
			Only matching content is extracted.
		
		Returns
		-------
		iterable of str
			List of absolute paths for the extracted content.
		"""
		with ZipFile(src, "r") as archive:
			files = archive.namelist()
			extracted = self.find_match_files(files, pattern)
			for e in extracted:
				archive.extract(e, dst)

		return extracted


	def unnest(
		self,
		src: str,
		tgt: str,
		delete: bool = True
	) -> None:
		"""
		Unnest all files in a source folder, and move them to a target
		folder.

		Parameters
		----------
		src : str
			Source folder inside which all children files are to be
			unnested.
		tgt : str
			Target folder in which unnested files are to be placed into.
		delete : bool, default `True`
			Whether or not to delete `src` after unnesting all files
			inside it and its sub-folders.
		"""
		file_name = os.path.basename(src)
		split_path = self.split_path(src)
		level = self.split_path(tgt)[-1]
		
		for i in range(len(split_path) - 1, 0, -1):
			is_level = split_path[i] == level
			if is_level:
				break
			level_to_delete = split_path.pop()
		
		joined_path = os.sep.join(split_path)
		unnested_path = os.path.join(joined_path, file_name)

		shutil.move(src, unnested_path)

		path_to_delete = os.path.join(joined_path, level_to_delete)
		_files = self.find_files(path_to_delete)

		if all([delete, os.path.isdir(path_to_delete), len(_files) == 0]):
			self.delete(path_to_delete)


	def extract(
		self,
		src: str,
		dst: str,
		unnest: bool = True,
		delete_archive: bool = False,
		pattern: Optional[str] = None
	) -> None:
		"""
		Extract content from a zip archive. Only content that match the
		provided regular expression is extracted.

		Parameters
		----------
		src : str
			Path to the archive.
		dst : str
			Path where the archive content is to be extracted.
		unnest: bool, default `True`
			Whether or not to unnest extracted files from their source 
			folder.
		delete_archive: bool, default `False`
			Whether or not to delete the source archive from the
			filesystem.
		pattern : str, optional
			Regular expression against which to match archive content. 
			Only matching content is extracted.
		"""
		_, src_ext = os.path.splitext(src)

		match src_ext:
			case ".7z":
				extracted = self.extract_7z(src, dst, pattern)
			case _:
				extracted = self.extract_zip(src, dst, pattern)

		if unnest:
			print(
				"Unnesting target files... "
				"You might be asked to accept deleting nested folders."
			)
			for e in extracted:
				nested_path = os.path.join(dst, e)
				self.unnest(nested_path, dst)
		
		if delete_archive:
			self.delete(src)

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
	"""
	GREEN = '\033[92m'
	RED = '\033[93m'
	BOLD = '\033[1m'
	END = '\033[0m'


@dataclass
class UrlManager:
	"""
	"""
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

	BDTOPO = (
		"https://wxs.ign.fr/"
		"859x8t863h6a09o9o6fy4v60/"
		"telechargement/prepackage/" 
		"BDTOPOV3-TOUSTHEMES-DEPARTEMENT_GPKG_PACK_221$" 
		"BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D{0}_{1}/" 
		"file/" 
		"BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D{0}_{1}.7z"
	)

	OSM = (
		"http://download.geofabrik.de/"
		"europe/"
		"france/"
		"{0}-latest-free.shp.zip"
	)


@dataclass
class PatternManager:
	"""
	"""
	ADMIN = r".*[/]COMMUNE[.].*"
	CORINE = r".*[/]CLC18_FR[.].*"
	BDTOPO = r".*gpkg$"
	OSM = r".*traffic_a_free_1[.].*"


class FileManager:
	"""
	"""
	def __init__(self, folder):
		"""
		"""
		self.folder = folder
	

	@staticmethod
	def _report(
		count: int,
		blockSize: int,
		totalSize: int
	) -> None:
		"""
		Source:
		https://ofstack.com/python/12319/method-that-shows-the-download-progress-when-python-downloads-a-file.html
		"""
		percent = int(count * blockSize * 100 / totalSize)
		sys.stdout.write(f"\r[{str(percent).rjust(3, '·')}%]")
		sys.stdout.flush()


	def find_files(
		self,
		path: str
	) -> Iterable[str]:
		"""
		"""
		files = []
		for dirpath, _, filenames in os.walk(path):
			for filename in filenames:
				files.append(os.path.join(dirpath, filename))
		return files


	def find_match_files(
		self,
		src: Union[str, Iterable[str]],
		pattern: str,
	) -> Iterable[str]:
		"""
		"""
		filter_pattern = re.compile(pattern)
		if isinstance(src, str):
			filtered = [f for f in os.listdir(src) if filter_pattern.match(f)]
		elif isinstance(src, list):
			filtered = [f for f in src if filter_pattern.match(f)]
		else:
			raise TypeError(f"Unsupported type ({type(src)})!")
		return filtered


	def delete(
		self,
		path: str
	) -> None:
		"""
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
		delete_zip: bool = False,
		pattern: Optional[str] = None
	) -> None:
		"""
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
		
		if delete_zip:
			self.delete(src)

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Protocol
from urllib.parse import unquote, urlparse

import boto3
import pdfplumber
from botocore.exceptions import ClientError
from lxml import etree
from pypdf import PdfReader

from trade_research.filings.models import (
    FilingDocument,
    ParsedDocument,
    ParsedPage,
    ParsedXbrlContext,
    ParsedXbrlFact,
)

XBRLI_NAMESPACE = "http://www.xbrl.org/2003/instance"
XBRLDI_NAMESPACE = "http://xbrl.org/2006/xbrldi"
PARSER_VERSION = "1.0.0"


class FilingParseError(RuntimeError):
    pass


class FilingArtifactStore(Protocol):
    def write_parsed_document(self, parsed: ParsedDocument) -> str: ...

    def read_parsed_document(self, artifact_uri: str) -> ParsedDocument: ...

    def parsed_document_uri(self, filing_id: str) -> str: ...

    def has_parsed_document(self, filing_id: str) -> bool: ...


class LocalFilingArtifactStore:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir.expanduser().resolve()

    def write_parsed_document(self, parsed: ParsedDocument) -> str:
        target_dir = self.base_dir / parsed.filing_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / "parsed_document.json"
        payload = parsed.model_dump(mode="json")
        payload["artifact_uri"] = target.as_uri()
        fd, temporary = tempfile.mkstemp(
            dir=target_dir,
            prefix=".parsed-",
            suffix=".json",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return target.as_uri()

    def parsed_document_uri(self, filing_id: str) -> str:
        return (self.base_dir / filing_id / "parsed_document.json").as_uri()

    def has_parsed_document(self, filing_id: str) -> bool:
        return (self.base_dir / filing_id / "parsed_document.json").is_file()

    def read_parsed_document(self, artifact_uri: str) -> ParsedDocument:
        path = file_uri_to_path(artifact_uri)
        if not path.is_relative_to(self.base_dir):
            raise ValueError("parsed artifact path escapes configured artifact directory")
        return ParsedDocument.model_validate_json(path.read_text(encoding="utf-8"))


class S3FilingArtifactStore:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        endpoint_url: str | None,
        region: str,
        access_key_id: str,
        secret_access_key: str,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
        self._ensure_bucket(region)

    def write_parsed_document(self, parsed: ParsedDocument) -> str:
        key = self._key(parsed.filing_id)
        uri = f"s3://{self.bucket}/{key}"
        payload = parsed.model_dump(mode="json")
        payload["artifact_uri"] = uri
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "filing-id": parsed.filing_id,
                "parser-version": parsed.parser_version,
            },
        )
        return uri

    def read_parsed_document(self, artifact_uri: str) -> ParsedDocument:
        parsed = urlparse(artifact_uri)
        if parsed.scheme != "s3" or parsed.netloc != self.bucket:
            raise ValueError("artifact URI is outside the configured S3 bucket")
        key = parsed.path.lstrip("/")
        if not key.startswith(f"{self.prefix}/"):
            raise ValueError("artifact URI is outside the configured S3 prefix")
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return ParsedDocument.model_validate_json(response["Body"].read())

    def parsed_document_uri(self, filing_id: str) -> str:
        return f"s3://{self.bucket}/{self._key(filing_id)}"

    def has_parsed_document(self, filing_id: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(filing_id))
            return True
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def _key(self, filing_id: str) -> str:
        return f"{self.prefix}/{filing_id}/parsed_document.json"

    def _ensure_bucket(self, region: str) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError:
            kwargs = {"Bucket": self.bucket}
            if region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": region
                }
            self.client.create_bucket(**kwargs)


class FilingParser:
    def __init__(
        self,
        *,
        artifact_store: FilingArtifactStore,
        max_document_bytes: int,
        pdf_max_pages: int,
        min_quality: float,
    ) -> None:
        self.artifact_store = artifact_store
        self.max_document_bytes = max_document_bytes
        self.pdf_max_pages = pdf_max_pages
        self.min_quality = min_quality

    def parse(self, document: FilingDocument) -> ParsedDocument:
        source_path = file_uri_to_path(document.object_uri)
        if not source_path.is_file():
            raise FilingParseError(f"filing source does not exist: {source_path}")
        if source_path.stat().st_size > self.max_document_bytes:
            raise FilingParseError(
                f"filing exceeds maximum document size: {source_path.stat().st_size}"
            )
        suffix = source_path.suffix.lower()
        if suffix == ".xml" or "xml" in document.content_type:
            parsed = self._parse_xbrl(document, source_path)
        elif suffix == ".pdf" or "pdf" in document.content_type:
            parsed = self._parse_pdf(document, source_path)
        else:
            raise FilingParseError(f"unsupported filing content type: {document.content_type}")
        artifact_uri = self.artifact_store.write_parsed_document(parsed)
        return parsed.model_copy(update={"artifact_uri": artifact_uri})

    def _parse_xbrl(self, document: FilingDocument, path: Path) -> ParsedDocument:
        parser = etree.XMLParser(
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
            recover=False,
            huge_tree=False,
            remove_comments=True,
        )
        try:
            root = etree.parse(str(path), parser=parser).getroot()
        except (etree.XMLSyntaxError, OSError) as exc:
            raise FilingParseError(f"unable to parse XBRL: {exc}") from exc

        contexts: dict[str, ParsedXbrlContext] = {}
        for context_element in root.findall(f"{{{XBRLI_NAMESPACE}}}context"):
            context_id = context_element.get("id")
            if not context_id:
                continue
            identifier = context_element.find(
                f".//{{{XBRLI_NAMESPACE}}}identifier"
            )
            start_element = context_element.find(
                f".//{{{XBRLI_NAMESPACE}}}startDate"
            )
            end_element = context_element.find(f".//{{{XBRLI_NAMESPACE}}}endDate")
            instant_element = context_element.find(
                f".//{{{XBRLI_NAMESPACE}}}instant"
            )
            dimensions: dict[str, str] = {}
            for member in context_element.findall(
                f".//{{{XBRLDI_NAMESPACE}}}explicitMember"
            ):
                dimension = member.get("dimension")
                if dimension:
                    dimensions[dimension] = (member.text or "").strip()
            for member in context_element.findall(
                f".//{{{XBRLDI_NAMESPACE}}}typedMember"
            ):
                dimension = member.get("dimension")
                if dimension:
                    dimensions[dimension] = " ".join(member.itertext()).strip()
            contexts[context_id] = ParsedXbrlContext(
                context_ref=context_id,
                entity_identifier=(identifier.text or "").strip()
                if identifier is not None
                else None,
                period_start=_element_date(start_element),
                period_end=_element_date(end_element),
                instant=_element_date(instant_element),
                dimensions=dimensions,
            )

        facts: list[ParsedXbrlFact] = []
        for element in root.iter():
            context_ref = element.get("contextRef")
            value_text = (element.text or "").strip()
            if not context_ref or not value_text:
                continue
            qname = etree.QName(element)
            facts.append(
                ParsedXbrlFact(
                    concept=qname.localname,
                    namespace=qname.namespace,
                    context_ref=context_ref,
                    unit_ref=element.get("unitRef"),
                    decimals=element.get("decimals"),
                    value_text=value_text,
                )
            )

        warnings: list[str] = []
        if not contexts:
            warnings.append("xbrl_no_contexts")
        if not facts:
            warnings.append("xbrl_no_facts")
        recognized_context_ratio = (
            sum(1 for fact in facts if fact.context_ref in contexts) / len(facts)
            if facts
            else 0.0
        )
        fact_score = min(len(facts) / 50.0, 1.0)
        parse_quality = round(0.55 * recognized_context_ratio + 0.45 * fact_score, 4)
        return ParsedDocument(
            filing_id=document.filing_id,
            content_type=document.content_type,
            parser_name="lxml-xbrl",
            parser_version=PARSER_VERSION,
            parse_quality=parse_quality,
            artifact_uri="pending://parsed",
            xbrl_contexts=contexts,
            xbrl_facts=facts,
            warnings=warnings,
        )

    def _parse_pdf(self, document: FilingDocument, path: Path) -> ParsedDocument:
        warnings: list[str] = []
        pages = self._pdfplumber_pages(path)
        quality = _pdf_quality(pages)
        parser_name = "pdfplumber"
        if quality < self.min_quality:
            fallback_pages = self._pypdf_pages(path)
            fallback_quality = _pdf_quality(fallback_pages)
            if fallback_quality > quality:
                pages = fallback_pages
                quality = fallback_quality
                parser_name = "pypdf-fallback"
                warnings.append("pdf_primary_parser_low_quality")
        if quality < self.min_quality:
            warnings.append("pdf_ocr_required")
        return ParsedDocument(
            filing_id=document.filing_id,
            content_type=document.content_type,
            parser_name=parser_name,
            parser_version=PARSER_VERSION,
            parse_quality=quality,
            artifact_uri="pending://parsed",
            pages=pages,
            warnings=warnings,
        )

    def _pdfplumber_pages(self, path: Path) -> list[ParsedPage]:
        pages: list[ParsedPage] = []
        try:
            with pdfplumber.open(path) as pdf:
                if len(pdf.pages) > self.pdf_max_pages:
                    raise FilingParseError(
                        f"PDF page count {len(pdf.pages)} exceeds limit {self.pdf_max_pages}"
                    )
                for number, page in enumerate(pdf.pages, start=1):
                    text = (page.extract_text(x_tolerance=2, y_tolerance=3) or "").strip()
                    pages.append(
                        ParsedPage(
                            page=number,
                            text=text,
                            character_count=len(text),
                        )
                    )
        except FilingParseError:
            raise
        except Exception as exc:
            raise FilingParseError(f"pdfplumber failed: {exc}") from exc
        return pages

    def _pypdf_pages(self, path: Path) -> list[ParsedPage]:
        try:
            reader = PdfReader(str(path), strict=False)
            if len(reader.pages) > self.pdf_max_pages:
                raise FilingParseError(
                    f"PDF page count {len(reader.pages)} exceeds limit {self.pdf_max_pages}"
                )
            pages = []
            for number, page in enumerate(reader.pages, start=1):
                text = (page.extract_text() or "").strip()
                pages.append(
                    ParsedPage(
                        page=number,
                        text=text,
                        character_count=len(text),
                    )
                )
            return pages
        except FilingParseError:
            raise
        except Exception as exc:
            raise FilingParseError(f"pypdf fallback failed: {exc}") from exc


def file_uri_to_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in {"", "file"}:
        raise ValueError("M1 accepts local file:// filing objects only")
    raw_path = unquote(parsed.path) if parsed.scheme == "file" else uri
    return Path(raw_path).expanduser().resolve()


def _element_date(element):
    if element is None or not element.text:
        return None
    from datetime import date

    return date.fromisoformat(element.text.strip()[:10])


def _pdf_quality(pages: list[ParsedPage]) -> float:
    if not pages:
        return 0.0
    nonempty = sum(1 for page in pages if page.character_count >= 40)
    nonempty_ratio = nonempty / len(pages)
    average_characters = sum(page.character_count for page in pages) / len(pages)
    density_score = min(average_characters / 500.0, 1.0)
    text = "".join(page.text for page in pages)
    printable_ratio = (
        sum(1 for character in text if character.isprintable()) / len(text)
        if text
        else 0.0
    )
    return round(
        0.45 * nonempty_ratio + 0.35 * density_score + 0.20 * printable_ratio,
        4,
    )

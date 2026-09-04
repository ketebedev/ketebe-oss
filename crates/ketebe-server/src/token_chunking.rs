use ketebe_core::{ChunkingStructure, TokenChunkingPolicy, TokenizerKind};
use std::collections::BTreeSet;

const CHUNKER_VERSION: &str = "token-structural-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSpan {
    pub start_byte: usize,
    pub end_byte: usize,
}

pub trait TokenCounter: Send + Sync {
    fn identity(&self) -> &'static str;
    fn spans(&self, text: &str) -> Vec<TokenSpan>;

    fn count(&self, text: &str) -> usize {
        self.spans(text).len()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnicodeWordTokenCounter;

impl TokenCounter for UnicodeWordTokenCounter {
    fn identity(&self) -> &'static str {
        "unicode_words_v1"
    }

    fn spans(&self, text: &str) -> Vec<TokenSpan> {
        let mut spans = Vec::new();
        let mut word_start = None;
        for (byte_index, ch) in text.char_indices() {
            let is_word = ch.is_alphanumeric() || ch == '_';
            if is_word {
                word_start.get_or_insert(byte_index);
                continue;
            }
            if let Some(start) = word_start.take() {
                spans.push(TokenSpan {
                    start_byte: start,
                    end_byte: byte_index,
                });
            }
            if !ch.is_whitespace() {
                spans.push(TokenSpan {
                    start_byte: byte_index,
                    end_byte: byte_index + ch.len_utf8(),
                });
            }
        }
        if let Some(start) = word_start {
            spans.push(TokenSpan {
                start_byte: start,
                end_byte: text.len(),
            });
        }
        spans
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredChunk {
    pub ordinal: usize,
    pub start_byte: usize,
    pub end_byte: usize,
    pub token_count: usize,
    pub text: String,
}

#[must_use]
pub fn chunker_fingerprint(policy: TokenChunkingPolicy) -> String {
    format!(
        "{CHUNKER_VERSION}:tokenizer={}:structure={}:max_tokens={}:overlap={}",
        tokenizer_identity(policy.tokenizer()),
        structure_identity(policy.structure()),
        policy.max_tokens(),
        policy.token_overlap(),
    )
}

#[must_use]
pub fn chunk_text_token_aware(text: &str, policy: TokenChunkingPolicy) -> Vec<StructuredChunk> {
    let counter = UnicodeWordTokenCounter;
    let tokens = counter.spans(text);
    if tokens.is_empty() {
        return Vec::new();
    }
    let preferred = preferred_breaks(text, &tokens, policy.structure());
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < tokens.len() {
        let hard_end = (start + policy.max_tokens()).min(tokens.len());
        let end = if hard_end == tokens.len() {
            hard_end
        } else {
            preferred
                .range((start + 1)..=hard_end)
                .next_back()
                .copied()
                .unwrap_or(hard_end)
        };
        let start_byte = tokens[start].start_byte;
        let end_byte = tokens[end - 1].end_byte;
        chunks.push(StructuredChunk {
            ordinal: chunks.len(),
            start_byte,
            end_byte,
            token_count: end - start,
            text: text[start_byte..end_byte].to_string(),
        });
        if end == tokens.len() {
            break;
        }
        let candidate = end.saturating_sub(policy.token_overlap());
        start = if candidate <= start { end } else { candidate };
    }
    chunks
}

fn preferred_breaks(
    text: &str,
    tokens: &[TokenSpan],
    structure: ChunkingStructure,
) -> BTreeSet<usize> {
    let boundaries = match structure {
        ChunkingStructure::Tokens => Vec::new(),
        ChunkingStructure::Sentences => sentence_boundaries(text),
        ChunkingStructure::Paragraphs => paragraph_boundaries(text),
        ChunkingStructure::Markdown => markdown_boundaries(text),
        ChunkingStructure::Html => html_boundaries(text),
    };
    boundaries
        .into_iter()
        .map(|boundary| {
            tokens
                .iter()
                .position(|token| token.start_byte >= boundary)
                .unwrap_or(tokens.len())
        })
        .filter(|index| *index > 0 && *index < tokens.len())
        .collect()
}

fn sentence_boundaries(text: &str) -> Vec<usize> {
    let mut boundaries = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if !matches!(ch, '.' | '!' | '?') {
            continue;
        }
        let after = index + ch.len_utf8();
        if chars.peek().is_none() || chars.peek().is_some_and(|(_, next)| next.is_whitespace()) {
            let mut boundary = after;
            while let Some((next_index, next)) = chars.peek().copied() {
                if !next.is_whitespace() {
                    break;
                }
                boundary = next_index + next.len_utf8();
                chars.next();
            }
            boundaries.push(boundary);
        }
    }
    boundaries
}

fn paragraph_boundaries(text: &str) -> Vec<usize> {
    let bytes = text.as_bytes();
    let mut boundaries = Vec::new();
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        if bytes[index] == b'\n' && bytes[index + 1] == b'\n' {
            let mut boundary = index + 2;
            while boundary < bytes.len() && bytes[boundary] == b'\n' {
                boundary += 1;
            }
            boundaries.push(boundary);
            index = boundary;
        } else {
            index += 1;
        }
    }
    boundaries
}

fn markdown_boundaries(text: &str) -> Vec<usize> {
    let mut boundaries = paragraph_boundaries(text);
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if offset > 0
            && (trimmed.starts_with('#')
                || trimmed.starts_with("- ")
                || trimmed.starts_with("* ")
                || trimmed.starts_with("> ")
                || trimmed.starts_with("```"))
        {
            boundaries.push(offset);
        }
        offset += line.len();
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

fn html_boundaries(text: &str) -> Vec<usize> {
    const BLOCK_ENDS: [&str; 10] = [
        "</p>",
        "</div>",
        "</section>",
        "</article>",
        "</li>",
        "</ul>",
        "</ol>",
        "</h1>",
        "</h2>",
        "</h3>",
    ];
    let lower = text.to_lowercase();
    let mut boundaries = Vec::new();
    for marker in BLOCK_ENDS {
        let mut search_from = 0usize;
        while search_from < lower.len() {
            let Some(relative) = lower[search_from..].find(marker) else {
                break;
            };
            let boundary = search_from + relative + marker.len();
            boundaries.push(boundary.min(text.len()));
            search_from = boundary;
        }
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

fn tokenizer_identity(kind: TokenizerKind) -> &'static str {
    match kind {
        TokenizerKind::UnicodeWordsV1 => "unicode_words_v1",
    }
}

fn structure_identity(structure: ChunkingStructure) -> &'static str {
    match structure {
        ChunkingStructure::Tokens => "tokens",
        ChunkingStructure::Sentences => "sentences",
        ChunkingStructure::Paragraphs => "paragraphs",
        ChunkingStructure::Markdown => "markdown",
        ChunkingStructure::Html => "html",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(
        structure: ChunkingStructure,
        max_tokens: usize,
        overlap: usize,
    ) -> TokenChunkingPolicy {
        TokenChunkingPolicy::new(
            structure,
            max_tokens,
            overlap,
            TokenizerKind::UnicodeWordsV1,
        )
        .unwrap()
    }

    #[test]
    fn unicode_tokenizer_is_deterministic() {
        let counter = UnicodeWordTokenCounter;
        let text = "İstanbul café — merhaba dünya! 你好 dünya";
        let first = counter.spans(text);
        assert_eq!(first, counter.spans(text));
        assert_eq!(counter.count(text), first.len());
        assert!(
            first
                .iter()
                .all(|span| text.is_char_boundary(span.start_byte))
        );
        assert!(
            first
                .iter()
                .all(|span| text.is_char_boundary(span.end_byte))
        );
    }

    #[test]
    fn token_chunks_respect_max_and_overlap() {
        let chunks = chunk_text_token_aware(
            "one two three four five six seven eight",
            policy(ChunkingStructure::Tokens, 4, 1),
        );
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| chunk.token_count <= 4));
        assert!(chunks[1].text.starts_with("four"));
    }

    #[test]
    fn structural_modes_prefer_boundaries() {
        let sentence = chunk_text_token_aware(
            "One two three. Four five six seven.",
            policy(ChunkingStructure::Sentences, 5, 0),
        );
        assert_eq!(sentence[0].text, "One two three.");

        let paragraph = chunk_text_token_aware(
            "alpha beta\n\ngamma delta epsilon",
            policy(ChunkingStructure::Paragraphs, 4, 0),
        );
        assert_eq!(paragraph[0].text, "alpha beta");
        let markdown = chunk_text_token_aware(
            "intro words\n# Heading\nnext words here",
            policy(ChunkingStructure::Markdown, 5, 0),
        );
        assert_eq!(markdown[0].text, "intro words");
        let html = chunk_text_token_aware(
            "<p>alpha beta</p><p>gamma delta</p>",
            policy(ChunkingStructure::Html, 8, 0),
        );
        assert!(html.len() >= 2);
    }

    #[test]
    fn fingerprint_captures_strategy_and_tokenizer_identity() {
        let value = chunker_fingerprint(policy(ChunkingStructure::Markdown, 128, 16));
        assert!(value.contains("tokenizer=unicode_words_v1"));
        assert!(value.contains("structure=markdown"));
        assert!(value.contains("max_tokens=128"));
    }
}

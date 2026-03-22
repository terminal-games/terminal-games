// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const MANIFEST_VERSION: u32 = 1;
const MAX_SHORTNAME_LEN: usize = 32;
const MAX_AUTHOR_LEN: usize = 256;
const MAX_VERSION_LEN: usize = 128;
const MAX_LOCALIZED_VALUE_LEN: usize = 16 * 1024;
const MAX_SCREENSHOT_CONTENT_LEN: usize = 128 * 1024;
const MAX_SCREENSHOTS_PER_LOCALE: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GameManifest {
    pub terminal_games_manifest_version: u32,
    pub shortname: String,
    #[serde(default)]
    pub details: GameDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GameDetails {
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub name: BTreeMap<String, String>,
    #[serde(default)]
    pub description: BTreeMap<String, String>,
    #[serde(default)]
    pub details: BTreeMap<String, String>,
    #[serde(default)]
    pub screenshots: BTreeMap<String, Vec<Screenshot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Screenshot {
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub caption: String,
}

impl GameManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.terminal_games_manifest_version != MANIFEST_VERSION {
            return Err(format!(
                "terminal_games_manifest_version must be {}",
                MANIFEST_VERSION
            ));
        }
        validate_shortname(&self.shortname)?;
        validate_author(&self.details.author)?;
        validate_version(&self.details.version)?;
        validate_localized_map("details.name", &self.details.name)?;
        validate_localized_map("details.description", &self.details.description)?;
        validate_localized_map("details.details", &self.details.details)?;
        for (locale, screenshots) in &self.details.screenshots {
            validate_locale(locale)?;
            if screenshots.len() > MAX_SCREENSHOTS_PER_LOCALE {
                return Err(format!(
                    "details.screenshots.{locale} exceeds {MAX_SCREENSHOTS_PER_LOCALE} entries"
                ));
            }
            for (index, screenshot) in screenshots.iter().enumerate() {
                if screenshot.content.len() > MAX_SCREENSHOT_CONTENT_LEN {
                    return Err(format!(
                        "details.screenshots.{locale}[{index}].content exceeds {} bytes",
                        MAX_SCREENSHOT_CONTENT_LEN
                    ));
                }
                if screenshot.caption.len() > MAX_LOCALIZED_VALUE_LEN {
                    return Err(format!(
                        "details.screenshots.{locale}[{index}].caption exceeds {} bytes",
                        MAX_LOCALIZED_VALUE_LEN
                    ));
                }
            }
        }
        Ok(())
    }
}

pub fn parse_and_validate_manifest_json(bytes: &[u8]) -> Result<GameManifest, String> {
    let manifest =
        serde_json::from_slice::<GameManifest>(bytes).map_err(|error| error.to_string())?;
    manifest.validate()?;
    Ok(manifest)
}

pub fn validate_shortname(shortname: &str) -> Result<(), String> {
    if shortname.is_empty() || shortname.len() > MAX_SHORTNAME_LEN {
        return Err(format!(
            "shortname must be between 1 and {} characters",
            MAX_SHORTNAME_LEN
        ));
    }
    if !shortname
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("shortname must contain only lowercase ASCII letters, digits, or '-'".into());
    }
    if !shortname
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err("shortname must start with a lowercase ASCII letter or digit".into());
    }
    if !shortname
        .bytes()
        .last()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err("shortname must end with a lowercase ASCII letter or digit".into());
    }
    Ok(())
}

fn validate_author(author: &str) -> Result<(), String> {
    if author.trim().is_empty() || author.len() > MAX_AUTHOR_LEN {
        return Err(format!(
            "details.author must be between 1 and {} bytes",
            MAX_AUTHOR_LEN
        ));
    }
    Ok(())
}

fn validate_version(version: &str) -> Result<(), String> {
    if version.trim().is_empty() || version.len() > MAX_VERSION_LEN {
        return Err(format!(
            "details.version must be between 1 and {} bytes",
            MAX_VERSION_LEN
        ));
    }
    Ok(())
}

fn validate_localized_map(path: &str, values: &BTreeMap<String, String>) -> Result<(), String> {
    for (locale, value) in values {
        validate_locale(locale)?;
        if value.len() > MAX_LOCALIZED_VALUE_LEN {
            return Err(format!(
                "{path}.{locale} exceeds {} bytes",
                MAX_LOCALIZED_VALUE_LEN
            ));
        }
    }
    Ok(())
}

fn validate_locale(locale: &str) -> Result<(), String> {
    if locale.trim().is_empty() {
        return Err("locale keys cannot be empty".into());
    }
    if !locale
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("locale keys must be ASCII alphanumeric plus '-' or '_'".into());
    }
    Ok(())
}

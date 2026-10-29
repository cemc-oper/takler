//! Optional client summary receiver. No GIL release, worker pool, unchecked model construction,
//! process-global view, or deferred validation. Python remains the header oracle.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::collections::HashSet;
use std::fmt;

pyo3::create_exception!(takler_summary_kernel, Fallback, PyValueError);
pyo3::create_exception!(takler_summary_kernel, AssemblyError, PyValueError);
pyo3::create_exception!(takler_summary_kernel, BudgetError, PyValueError);

// serde_json::Value alone overwrites duplicate keys. This visitor checks every
// object after unescaping its keys, including nested/unknown fields.
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(Value::Number(v.into())))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(Value::Number(v.into())))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Strict, E> {
                Number::from_f64(v)
                    .map(|n| Strict(Value::Number(n)))
                    .ok_or_else(|| E::custom("number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Strict, E> {
                Ok(Strict(Value::String(v)))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut out = Vec::new();
                while let Some(Strict(v)) = a.next_element()? {
                    out.push(v);
                }
                Ok(Strict(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut out = Map::new();
                while let Some((k, Strict(v))) = a.next_entry::<String, Strict>()? {
                    if out.insert(k, v).is_some() {
                        return Err(de::Error::custom("duplicate key"));
                    }
                }
                Ok(Strict(Value::Object(out)))
            }
        }
        d.deserialize_any(V)
    }
}

fn fallback() -> PyErr {
    Fallback::new_err("use original decoder")
}
fn string<'a>(m: &'a Map<String, Value>, k: &str) -> PyResult<&'a str> {
    m.get(k).and_then(Value::as_str).ok_or_else(fallback)
}
fn path_valid(p: &str) -> bool {
    p == "/"
        || (p.starts_with('/')
            && !p.ends_with('/')
            && p[1..].split('/').all(|s| {
                !s.is_empty() && s != "." && s != ".." && !s.chars().any(|c| c <= '\u{1f}')
            }))
}
struct Node {
    path: String,
    kind: String,
    type_id: String,
    status: String,
    suspended: bool,
    attempt: Option<String>,
    unknown: Option<bool>,
}
impl Node {
    fn from_json(v: &Value) -> PyResult<Self> {
        let m = v.as_object().ok_or_else(fallback)?;
        if m.keys().any(|k| {
            ![
                "path",
                "node_kind",
                "type_id",
                "status",
                "suspended",
                "attempt_id",
                "submission_unknown",
            ]
            .contains(&k.as_str())
        }) {
            return Err(fallback());
        }
        let path = string(m, "path")?;
        let kind = string(m, "node_kind")?;
        let type_id = string(m, "type_id")?;
        let status = string(m, "status")?;
        let suspended = m
            .get("suspended")
            .and_then(Value::as_bool)
            .ok_or_else(fallback)?;
        let attempt = match m.get("attempt_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            _ => return Err(fallback()),
        };
        let unknown = match m.get("submission_unknown") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(*b),
            _ => return Err(fallback()),
        };
        if !path_valid(path)
            || !["bunch", "flow", "container", "task"].contains(&kind)
            || ![
                "unknown",
                "queued",
                "submitted",
                "active",
                "complete",
                "aborted",
            ]
            .contains(&status)
            || type_id.is_empty()
            || attempt
                .as_ref()
                .is_some_and(|s| kind != "task" || s.is_empty())
            || unknown
                .is_some_and(|b| !b || kind != "task" || status != "submitted" || attempt.is_none())
        {
            return Err(fallback());
        }
        Ok(Self {
            path: path.into(),
            kind: kind.into(),
            type_id: type_id.into(),
            status: status.into(),
            suspended,
            attempt,
            unknown,
        })
    }
    fn from_model(v: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            path: v.getattr("path")?.extract()?,
            kind: v.getattr("node_kind")?.extract()?,
            type_id: v.getattr("type_id")?.extract()?,
            status: v.getattr("status")?.extract()?,
            suspended: v.getattr("suspended")?.extract()?,
            attempt: v.getattr("attempt_id")?.extract()?,
            unknown: v.getattr("submission_unknown")?.extract()?,
        })
    }
}

fn to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::Null => py.None().into_bound(py),
        Value::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
        Value::String(s) => s.into_pyobject(py)?.into_any(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py)?.into_any()
            } else if let Some(i) = n.as_u64() {
                i.into_pyobject(py)?.into_any()
            } else {
                n.as_f64()
                    .ok_or_else(fallback)?
                    .into_pyobject(py)?
                    .into_any()
            }
        }
        Value::Array(a) => {
            let l = PyList::empty(py);
            for v in a {
                l.append(to_py(py, v)?)?;
            }
            l.into_any()
        }
        Value::Object(m) => {
            let d = PyDict::new(py);
            for (k, v) in m {
                d.set_item(k, to_py(py, v)?)?;
            }
            d.into_any()
        }
    })
}

#[pyclass]
struct Kernel {
    pending: Vec<Node>,
    nodes: Py<PyDict>,
    order: Py<PyList>,
    scope: String,
    depth: Option<usize>,
    size: usize,
    old_size: usize,
    limit: usize,
    node_class: Py<PyAny>,
    native_pages: usize,
    fallback_pages: usize,
}
#[pymethods]
impl Kernel {
    #[new]
    #[pyo3(signature=(scope, depth, old_size, limit, node_class))]
    fn new(
        py: Python<'_>,
        scope: String,
        depth: Option<usize>,
        old_size: usize,
        limit: usize,
        node_class: Py<PyAny>,
    ) -> Self {
        Self {
            pending: Vec::new(),
            nodes: PyDict::new(py).unbind(),
            order: PyList::empty(py).unbind(),
            scope,
            depth,
            size: 1024,
            old_size,
            limit,
            node_class,
            native_pages: 0,
            fallback_pages: 0,
        }
    }

    // Return ONLY header data for the original Python header validator. The
    // actual nodes are checked here, retained privately, and consumed by append.
    fn prepare<'py>(&mut self, py: Python<'py>, raw: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.pending.clear();
        if raw.len() > 262_144 {
            return Err(fallback());
        }
        let Strict(value) = serde_json::from_slice::<Strict>(raw).map_err(|_| fallback())?;
        let Value::Object(mut header) = value else {
            return Err(fallback());
        };
        if string(&header, "kind")? != "page" {
            return Err(fallback());
        }
        let vals = header.remove("nodes").ok_or_else(fallback)?;
        let vals = vals.as_array().ok_or_else(fallback)?;
        if vals.len() > 2048 {
            return Err(fallback());
        }
        let gens = header
            .get("flow_generations")
            .and_then(Value::as_object)
            .ok_or_else(fallback)?;
        let mut seen = HashSet::new();
        let mut pending = Vec::with_capacity(vals.len());
        for v in vals {
            let n = Node::from_json(v)?;
            if !seen.insert(n.path.clone()) {
                return Err(fallback());
            }
            if n.path != "/"
                && !gens.contains_key(&format!("/{}", n.path.split('/').nth(1).unwrap()))
            {
                return Err(fallback());
            }
            pending.push(n);
        }
        // A root witness preserves the nonempty-page check in PageReply. Its
        // node checks are NOT evidence for actual nodes: those were all checked
        // above (including each node's generation membership and duplicates).
        header.insert("nodes".into(), Value::Array(if pending.is_empty() { vec![] } else {
            vec![serde_json::json!({"path":"/", "node_kind":"bunch", "type_id":"header-witness", "status":"unknown", "suspended":false})]
        }));
        self.pending = pending;
        self.native_pages += 1;
        to_py(py, &Value::Object(header))
    }

    fn stage_validated(&mut self, nodes: &Bound<'_, PyAny>) -> PyResult<()> {
        self.pending.clear();
        for n in nodes.try_iter()? {
            self.pending.push(Node::from_model(&n?)?);
        }
        self.fallback_pages += 1;
        Ok(())
    }

    fn append(&mut self, py: Python<'_>) -> PyResult<()> {
        let nodes = self.nodes.bind(py);
        let order = self.order.bind(py);
        let prefix = format!("{}/", self.scope.trim_end_matches('/'));
        let scope_depth = if self.scope == "/" {
            0
        } else {
            self.scope.matches('/').count()
        };
        for n in self.pending.drain(..) {
            let path = PyString::new(py, &n.path);
            if nodes.contains(&path)? {
                return Err(AssemblyError::new_err("duplicate query node"));
            }
            if n.path != self.scope && !n.path.starts_with(&prefix) {
                return Err(AssemblyError::new_err("query node outside requested scope"));
            }
            let d = if n.path == "/" {
                0
            } else {
                n.path.matches('/').count()
            };
            if self.depth.is_some_and(|max| d - scope_depth > max) {
                return Err(AssemblyError::new_err("query node outside requested depth"));
            }
            let parent = n.path.rsplit_once('/').unwrap().0;
            let parent = if parent.is_empty() { "/" } else { parent };
            if n.path != self.scope && !nodes.contains(parent)? {
                return Err(AssemblyError::new_err("query parent is missing"));
            }
            self.size += 512
                + n.path.len()
                + n.kind.len()
                + n.type_id.len()
                + n.status.len()
                + n.attempt.as_ref().map_or(0, String::len);
            if self.size.saturating_add(self.old_size) > self.limit {
                return Err(BudgetError::new_err("query view memory budget exceeded"));
            }
            // Use the ORIGINAL public dataclass constructor. No substitute native
            // view or unchecked Python allocation; this cost belongs in timing.
            let row = self.node_class.bind(py).call1((
                &path,
                n.kind,
                n.type_id,
                n.status,
                n.suspended,
                n.attempt,
                n.unknown,
            ))?;
            nodes.set_item(&path, row)?;
            order.append(&path)?;
        }
        Ok(())
    }

    fn finish<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyTuple>, usize)> {
        let order = self.order.bind(py);
        if order.is_empty() || order.get_item(0)?.extract::<String>()? != self.scope {
            return Err(AssemblyError::new_err("query scope root is missing"));
        }
        Ok((self.nodes.bind(py).clone(), order.to_tuple(), self.size))
    }

    fn counts(&self) -> (usize, usize) {
        (self.native_pages, self.fallback_pages)
    }
}

#[pymodule]
fn takler_summary_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("API_VERSION", 1)?;
    m.add_class::<Kernel>()?;
    m.add("Fallback", m.py().get_type::<Fallback>())?;
    m.add("AssemblyError", m.py().get_type::<AssemblyError>())?;
    m.add("BudgetError", m.py().get_type::<BudgetError>())?;
    Ok(())
}

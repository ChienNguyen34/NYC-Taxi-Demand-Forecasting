# 📚 Git Sync Summary - Lấy Code Mới Từ Remote Repository

## 🎯 **Tình huống:**
- Member khác đã commit code mới lên GitHub
- Bạn có local changes chưa commit
- Muốn lấy code mới từ remote và xóa hết local changes

---

## 🔄 **Các bước đã thực hiện:**

### **Step 1: Kiểm tra trạng thái Git**

```bash
git status
```

**Giải thích:**
- **`git status`**: Hiển thị trạng thái hiện tại của working directory
- Shows:
  - Files đã modify nhưng chưa stage
  - Files đã stage nhưng chưa commit
  - Untracked files (files mới tạo chưa được git theo dõi)
  - Branch hiện tại đang ở đâu

**Output nhận được:**
```
On branch main
Changes not staged for commit:
  modified:   airflow_dags/nyc_taxi_dag.py
  modified:   streaming_simulation/setup_streaming.py
  deleted:    ARCHITECTURE.md
  ...

Untracked files:
  cloudbuild.yaml
  doc/
  streaming_simulation/populate_bigquery.py
  ...
```

---

### **Step 2: Fetch changes từ remote**

```bash
git fetch origin
```

**Giải thích từng phần:**
- **`git`**: Git command-line tool
- **`fetch`**: Download changes từ remote repository (không merge vào local)
- **`origin`**: Tên của remote repository (mặc định khi clone từ GitHub)

**Cách hoạt động:**
```
Remote (GitHub)           Local Repository
├── main branch      →    ├── origin/main (remote tracking branch)
└── commits          →    └── .git/objects/ (downloaded objects)
```

**Khác với `git pull`:**
- `git fetch`: CHỈ download, KHÔNG merge
- `git pull` = `git fetch` + `git merge`

---

### **Step 3: Xem commits mới trên remote**

```bash
git log origin/main --oneline -5
```

**Giải thích từng phần:**
- **`git log`**: Xem lịch sử commits
- **`origin/main`**: Branch `main` trên remote `origin`
- **`--oneline`**: Hiển thị mỗi commit trên 1 dòng (compact format)
- **`-5`**: Chỉ hiển thị 5 commits gần nhất

**Output nhận được:**
```
58f5387 (origin/main, origin/HEAD) Weather streaming + usecase1: weather plotting + fare prediction
9f4fea6 Add usecase + weather streaming
fade7bc (HEAD -> main) Fix: Updated staging model to use location_id schema
```

**Phân tích output:**
- `58f5387`: Commit hash (7 ký tự đầu)
- `(origin/main)`: Branch này trên remote
- `(HEAD -> main)`: Vị trí hiện tại của local branch
- **→ Remote có 2 commits mới hơn local!**

---

### **Step 4: Reset hard về remote (Option 3 - Discard all local changes)**

```bash
git reset --hard origin/main
```

**⚠️ WARNING: Command này XÓA TẤT CẢ LOCAL CHANGES!**

**Giải thích từng phần:**
- **`git reset`**: Di chuyển HEAD và branch pointer
- **`--hard`**: Reset 3 areas:
  1. **HEAD**: Move to target commit
  2. **Staging Area (Index)**: Reset về target commit
  3. **Working Directory**: Xóa hết changes, restore về target commit
- **`origin/main`**: Target commit (latest commit trên remote)

**3 modes của git reset:**
```
git reset --soft origin/main   # Chỉ move HEAD, giữ nguyên staging & working
git reset --mixed origin/main  # Move HEAD + reset staging, giữ working (default)
git reset --hard origin/main   # Reset TẤT CẢ ⚠️
```

**Trước reset:**
```
Working Directory:  [modified files + untracked files]
Staging Area:       [staged changes]
HEAD:              fade7bc (local commit)
```

**Sau reset:**
```
Working Directory:  [CLEAN - matching 58f5387] ✅
Staging Area:       [EMPTY] ✅
HEAD:              58f5387 (same as origin/main) ✅
```

---

### **Step 5: Xóa untracked files**

```bash
git clean -fd
```

**Giải thích từng phần:**
- **`git clean`**: Xóa untracked files (files git không theo dõi)
- **`-f`**: Force (bắt buộc phải có flag này để xác nhận)
- **`-d`**: Directories (xóa cả folders untracked)

**Files đã xóa:**
```
Removing cloudbuild.yaml
Removing doc/
Removing streaming_simulation/doc/
Removing streaming_simulation/populate_bigquery.py
Removing streaming_simulation/streaming_config.yaml
Removing streaming_simulation/test/
Removing sync_to_airflow.ps1
Removing sync_to_airflow.sh
```

**Các flags khác của `git clean`:**
```bash
git clean -n    # Dry run (preview files sẽ xóa, không xóa thật)
git clean -x    # Xóa cả ignored files (trong .gitignore)
git clean -X    # CHỈ xóa ignored files
git clean -i    # Interactive mode
```

---

### **Step 6: Verify clean state**

```bash
git status
```

**Output cuối cùng:**
```
On branch main
nothing to commit, working tree clean
```

✅ **Perfect!** Working directory hoàn toàn sạch, match 100% với remote.

---

## 📊 **So sánh 3 Options đã đề xuất:**

### **Option 1: Stash → Pull → Pop (SAFE)**

```bash
git stash push -m "My changes"    # Lưu changes vào stash
git pull origin main               # Lấy code mới
git stash pop                      # Apply lại changes
```

**Ưu điểm:**
- ✅ Giữ được local changes
- ✅ Có thể áp dụng lại sau
- ✅ Safe, không mất code

**Nhược điểm:**
- ⚠️ Có thể gặp conflicts khi pop
- ⚠️ Phức tạp hơn

---

### **Option 2: Commit → Pull → Merge (STANDARD)**

```bash
git add .
git commit -m "WIP: My changes"
git pull origin main
```

**Ưu điểm:**
- ✅ Changes được lưu trong git history
- ✅ Có thể revert về sau
- ✅ Standard workflow

**Nhược điểm:**
- ⚠️ Tạo merge commit
- ⚠️ Git history dài hơn
- ⚠️ Có thể conflicts

---

### **Option 3: Reset Hard + Clean (DESTRUCTIVE)** ← BẠN ĐÃ CHỌN

```bash
git reset --hard origin/main
git clean -fd
```

**Ưu điểm:**
- ✅ Đơn giản, nhanh
- ✅ 100% clean, match remote
- ✅ Không có conflicts

**Nhược điểm:**
- ❌ MẤT TẤT CẢ LOCAL CHANGES
- ❌ Không thể undo
- ❌ Nguy hiểm nếu có code quan trọng

---

## 🎓 **Git Concepts Quan Trọng:**

### **1. Git Areas:**

```
┌─────────────────────────────────────────────────┐
│ Working Directory                               │
│ (files bạn đang làm việc)                      │
│                                                  │
│  ↓ git add                                      │
├─────────────────────────────────────────────────┤
│ Staging Area (Index)                            │
│ (files sẵn sàng commit)                        │
│                                                  │
│  ↓ git commit                                   │
├─────────────────────────────────────────────────┤
│ Local Repository (.git/)                        │
│ (commit history)                                │
│                                                  │
│  ↓ git push                                     │
├─────────────────────────────────────────────────┤
│ Remote Repository (GitHub)                      │
│ (shared with team)                              │
└─────────────────────────────────────────────────┘
```

### **2. Branches:**

```
origin/main (remote)  →  58f5387 ←─── Newest commit
                            ↑
local main (before)   →  fade7bc
                            ↑
                         9f4fea6
                            ↑
                         07dc903 ←─── Initial commit
```

**Sau `git reset --hard origin/main`:**

```
origin/main (remote)  →  58f5387
                            ↑
local main (after)    →  58f5387 ←─── Now matching!
```

### **3. HEAD:**

- **HEAD**: Pointer to current commit you're on
- **`HEAD -> main`**: You're on branch `main`
- **Detached HEAD**: HEAD points directly to commit (không qua branch)

---

## 🔍 **Useful Git Commands để check:**

```bash
# Xem differences giữa local và remote
git diff main origin/main

# Xem files changed trong commit cụ thể
git show 58f5387 --name-only

# Xem detailed changes
git show 58f5387

# Xem git log dạng graph
git log --graph --oneline --all

# Xem stash list (nếu dùng Option 1)
git stash list

# Undo git reset (nếu còn trong reflog)
git reflog                    # Tìm commit hash trước khi reset
git reset --hard <hash>       # Restore về commit đó
```

---

## 📋 **Checklist khi sync với remote:**

- [x] **1. Check status:** `git status` - Xem có changes nào
- [x] **2. Fetch updates:** `git fetch origin` - Download mà không merge
- [x] **3. Review changes:** `git log origin/main` - Xem commits mới
- [x] **4. Decide strategy:**
  - Option 1: Stash (giữ changes)
  - Option 2: Commit (lưu vào history)
  - Option 3: Reset hard (xóa changes) ← Đã chọn
- [x] **5. Execute:** `git reset --hard origin/main`
- [x] **6. Clean untracked:** `git clean -fd`
- [x] **7. Verify:** `git status` - Confirm clean state

---

## 🎯 **Kết quả cuối cùng:**

```
✅ Working tree clean
✅ No uncommitted changes
✅ Local branch = Remote branch (58f5387)
✅ All untracked files removed
✅ Ready to work with latest code from team
```

---

## 📚 **Tài liệu tham khảo:**

- [Git Reset Explained](https://git-scm.com/docs/git-reset)
- [Git Clean Documentation](https://git-scm.com/docs/git-clean)
- [Understanding Git Areas](https://git-scm.com/book/en/v2/Git-Basics-Recording-Changes-to-the-Repository)

---

**💡 Lưu ý quan trọng:**
- Luôn `git fetch` trước khi `git reset` để đảm bảo có latest remote changes
- Chỉ dùng `git reset --hard` khi **chắc chắn** không cần local changes
- Có thể recover từ `git reflog` trong vài ngày nếu nhớ commit hash
- Best practice: Commit hoặc stash changes trước khi pull code mới

**🎉 Bây giờ bạn đã có code mới nhất từ team member!**
